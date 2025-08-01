use anyhow::{Context, Result, anyhow};
use axum::{
    Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::patch,
};
use clap::Parser;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    Client, Config,
    api::{Api, Patch, PatchParams},
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use tokio::net::TcpListener;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
struct ConfigMapRef {
    namespace: String,
    name: String,
}

impl std::str::FromStr for ConfigMapRef {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() == 2 {
            Ok(ConfigMapRef {
                namespace: parts[0].to_string(),
                name: parts[1].to_string(),
            })
        } else {
            Err(anyhow!(
                "Invalid ConfigMap format: '{}' (expected namespace/name)",
                s
            ))
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "git-sync-reloader-adapter",
    about = "Webhook adapter to connect git-sync with Stakater Reloader",
    version
)]
struct Args {
    /// ConfigMaps to allow updates for (format: namespace/name)
    #[arg(
        required = true,
        help = "ConfigMaps to allow updates for in namespace/name format"
    )]
    configmaps: Vec<String>,

    /// Port to listen on
    #[arg(short, long, default_value = "8080")]
    port: u16,

    /// Address to bind to
    #[arg(short, long, default_value = "0.0.0.0")]
    addr: String,
}

#[derive(Clone)]
struct AppState {
    kube_client: Client,
    allowed_configmaps: BTreeSet<ConfigMapRef>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Parse allowed ConfigMaps from CLI args
    let mut allowed_configmaps = BTreeSet::new();
    for cm in &args.configmaps {
        let cm_ref = cm.parse().context("failed to parse config map names")?;
        allowed_configmaps.insert(cm_ref);
    }

    // Create Kubernetes client
    let config = Config::infer()
        .await
        .context("failed to infer kube config")?;
    let client = Client::try_from(config).context("failed to connect to kube")?;

    let state = AppState {
        kube_client: client,
        allowed_configmaps,
    };

    // Build the application router
    let app = Router::new()
        .route("/webhook/:namespace/:configmap", patch(handle_webhook))
        .with_state(state);

    // Start the server
    let bind_addr = format!("{}:{}", args.addr, args.port);
    let listener = TcpListener::bind(&bind_addr)
        .await
        .context("failed to bind server address")?;
    println!("Webhook adapter listening on {bind_addr}");

    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_webhook(
    Path((namespace, configmap_name)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, StatusCode> {
    println!("Received webhook for {namespace}/{configmap_name}");

    // Check if this ConfigMap is allowed
    let cm_ref = ConfigMapRef {
        namespace: namespace.clone(),
        name: configmap_name.clone(),
    };

    if !state.allowed_configmaps.contains(&cm_ref) {
        println!("Denied update to unauthorized ConfigMap: {namespace}/{configmap_name}");
        return Err(StatusCode::FORBIDDEN);
    }

    // Extract the git sync hash from headers
    let header = headers
        .get("Gitsync-Hash")
        .or_else(|| headers.get("gitsync-hash"))
        .and_then(|h| h.to_str().ok());

    let git_hash = match header {
        Some(hash) => hash,
        None => {
            println!("Request missing Gitsync-Hash header for: {namespace}/{configmap_name}");
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    println!("Git hash: {git_hash}");

    // Update the ConfigMap
    match update_configmap(&state.kube_client, &namespace, &configmap_name, git_hash).await {
        Ok(updated) => {
            if updated {
                println!("Successfully updated ConfigMap {namespace}/{configmap_name}");
            } else {
                println!("ConfigMap {namespace}/{configmap_name} already up to date");
            }
            Ok(Json(json!({
                "status": "success",
                "git_hash": git_hash,
                "updated": updated
            })))
        }
        Err(e) => {
            eprintln!("Failed to update ConfigMap: {e:?}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn update_configmap(
    client: &Client,
    namespace: &str,
    name: &str,
    git_hash: &str,
) -> Result<bool> {
    let configmaps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);

    // Check if the hash is different from current
    let current = configmaps
        .get(name)
        .await
        .context("failed to load current config map value")?;
    let current_hash = current
        .metadata
        .annotations
        .as_ref()
        .and_then(|ann| ann.get("git-sync-hash"))
        .map(|s| s.as_str());

    if current_hash == Some(git_hash) {
        println!("Git hash unchanged ({git_hash}), skipping update");
        return Ok(false);
    }

    // Create the patch to update annotations
    let mut annotations = BTreeMap::new();
    annotations.insert("git-sync-hash".to_string(), git_hash.to_string());

    let patch = json!({
        "metadata": {
            "annotations": annotations
        }
    });

    let patch_params = PatchParams::apply("git-sync-webhook-adapter");

    configmaps
        .patch(name, &patch_params, &Patch::Merge(&patch))
        .await
        .context("failed to apply config path")?;

    Ok(true)
}
