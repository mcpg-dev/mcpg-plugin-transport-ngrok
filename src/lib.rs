//! `dev.mcpg.transport.ngrok` — serve the gateway at an ngrok endpoint.
//!
//! An `http`-wire transport: the plugin opens an ngrok session (outbound
//! only, no inbound port), listens on an HTTP endpoint, and hands every
//! request to the gateway, whose own router answers it. Identity, OAuth
//! discovery, per-IP rate limits (keyed on the client address ngrok
//! reports), SSE and both MCP revisions behave as on the HTTP listener.
//!
//! ngrok's session reconnects by itself after a network failure and binds
//! the endpoint again. The endpoint URL is the transport's listen address;
//! the gateway uses it as its public address for webhook callbacks.

use std::sync::mpsc;
use std::time::Duration;

use futures_util::StreamExt as _;
use mcpg_plugin_http_ingress::{Connection, serve_connection};
use mcpg_plugin_protocol::transport::{TransportError, TransportWire};
use mcpg_plugin_protocol::{PluginClass, PluginManifest};
use mcpg_plugin_sdk::declare_plugin;
use mcpg_plugin_sdk::ffi::{
    SyncMessageDispatcher, SyncTransport, SyncTransportHandle, WatchHandleBox,
};
use mcpg_plugin_sdk::http_ingress::HttpIngressServer;
use ngrok::Session;
use ngrok::config::TunnelBuilder as _;
use ngrok::conn::ConnInfo as _;
use ngrok::tunnel::{EndpointInfo as _, HttpTunnel, TunnelCloser as _};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

mod config;

pub use config::NgrokConfig;

const PLUGIN_ID: &str = "dev.mcpg.transport.ngrok";

/// How long dropping the transport waits for its connections to end.
const DRAIN_LIMIT: Duration = Duration::from_secs(5);
/// The first pause after a failed accept; it doubles per failure up to
/// [`ACCEPT_RETRY_MAX`], so a broken endpoint neither spins nor floods the log.
const ACCEPT_RETRY: Duration = Duration::from_millis(200);
const ACCEPT_RETRY_MAX: Duration = Duration::from_secs(5);

pub struct NgrokTransport {
    manifest: PluginManifest,
    /// The `plugins[]` entry config; used when the `transports[]` entry
    /// has none.
    plugin_config: serde_json::Value,
    runtime: Option<Runtime>,
}

impl NgrokTransport {
    fn new(plugin_config: &str) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("mcpg-ngrok")
            .enable_all()
            .build()
            .expect("transport.ngrok: failed to build tokio runtime");
        Self {
            manifest: PluginManifest {
                id: PLUGIN_ID.into(),
                version: env!("CARGO_PKG_VERSION").into(),
                name: "ngrok Transport".into(),
                plugin_class: PluginClass::Transport,
                protocol_version: "1.0".into(),
                license: None,
                required_capabilities: Vec::new(),
                tags: Vec::new(),
                provides: Vec::new(),
                provides_schemes: Vec::new(),
                module_path_prefix: ::std::module_path!()
                    .split("::")
                    .next()
                    .unwrap_or("")
                    .to_owned(),
                backend_profile: None,
            },
            plugin_config: serde_json::from_str(plugin_config).unwrap_or(serde_json::Value::Null),
            runtime: Some(runtime),
        }
    }

    fn runtime(&self) -> &tokio::runtime::Handle {
        self.runtime
            .as_ref()
            .expect("the runtime lives until the plugin drops")
            .handle()
    }

    /// The listener config, else the plugin entry's config.
    fn config_for(&self, listener_config: &serde_json::Value) -> Result<NgrokConfig, String> {
        let empty = match listener_config {
            serde_json::Value::Null => true,
            serde_json::Value::Object(map) => map.is_empty(),
            _ => false,
        };
        NgrokConfig::from_value(if empty {
            &self.plugin_config
        } else {
            listener_config
        })
    }
}

impl Drop for NgrokTransport {
    fn drop(&mut self) {
        // The host may drop the plugin on one of its runtime threads, where
        // a blocking runtime drop panics.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// One open endpoint: the state behind a transport handle.
struct Endpoint {
    url: String,
    /// Stops accepting connections and closes the endpoint.
    stop: CancellationToken,
    /// Ends the connections still open.
    kill: CancellationToken,
    tasks: TaskTracker,
    runtime: tokio::runtime::Handle,
}

impl SyncTransport for NgrokTransport {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn name(&self) -> &str {
        "ngrok-v1"
    }

    fn wire(&self) -> TransportWire {
        TransportWire::Http
    }

    fn start(
        &self,
        _listener_config: &serde_json::Value,
        _dispatcher: std::sync::Arc<dyn SyncMessageDispatcher>,
    ) -> Result<SyncTransportHandle, TransportError> {
        Err(TransportError::InvalidConfig {
            message: format!("{PLUGIN_ID} hands the gateway HTTP requests, not JSON-RPC messages"),
        })
    }

    fn start_http(
        &self,
        listener_config: &serde_json::Value,
        server: HttpIngressServer,
    ) -> Result<SyncTransportHandle, TransportError> {
        let config = self
            .config_for(listener_config)
            .map_err(|message| TransportError::InvalidConfig { message })?;
        let runtime = self.runtime().clone();
        let timeout = config.connect_timeout();
        let (opened_tx, opened_rx) = mpsc::channel();
        let open_config = config.clone();
        runtime.spawn(async move {
            let opened = tokio::time::timeout(timeout, open(&open_config))
                .await
                .unwrap_or_else(|_| {
                    Err(format!(
                        "no ngrok endpoint within {} s",
                        open_config.connect_timeout_secs
                    ))
                });
            let _ = opened_tx.send(opened);
        });
        let (session, tunnel) = opened_rx
            .recv_timeout(timeout + Duration::from_secs(5))
            .map_err(|_| TransportError::BindFailed {
                reason: "the ngrok session did not answer".into(),
            })?
            .map_err(|reason| TransportError::BindFailed {
                reason: redact(&reason, &config.authtoken),
            })?;
        let endpoint = Endpoint {
            url: tunnel.url().to_owned(),
            stop: CancellationToken::new(),
            kill: CancellationToken::new(),
            tasks: TaskTracker::new(),
            runtime: runtime.clone(),
        };
        tracing::info!(plugin_id = PLUGIN_ID, url = %endpoint.url, "ngrok endpoint open");
        let accept = accept_loop(
            session,
            tunnel,
            server,
            endpoint.stop.clone(),
            endpoint.kill.clone(),
            endpoint.tasks.clone(),
            config.max_request_body_bytes,
        );
        endpoint.tasks.spawn_on(accept, &runtime);
        let url = endpoint.url.clone();
        Ok(SyncTransportHandle {
            handle: Box::into_raw(Box::new(endpoint)).cast(),
            listen_address: Some(url),
        })
    }

    fn transport_handle_close(&self, transport_handle: WatchHandleBox) {
        // SAFETY: the host passes back the handle `start_http` returned,
        // and frees it only through `transport_handle_drop`.
        let endpoint = unsafe { &*(transport_handle.0 as *const Endpoint) };
        endpoint.stop.cancel();
    }

    fn transport_handle_drop(&self, transport_handle: WatchHandleBox) {
        // SAFETY: as in `transport_handle_close`; the host calls this once.
        let endpoint = unsafe { Box::from_raw(transport_handle.0.cast::<Endpoint>()) };
        endpoint.stop.cancel();
        endpoint.kill.cancel();
        endpoint.tasks.close();
        // The host frees its server once this returns, so every task that
        // can call it must be gone first.
        let (done_tx, done_rx) = mpsc::channel();
        let tasks = endpoint.tasks.clone();
        endpoint.runtime.spawn(async move {
            tasks.wait().await;
            let _ = done_tx.send(());
        });
        if done_rx.recv_timeout(DRAIN_LIMIT).is_err() {
            tracing::warn!(
                plugin_id = PLUGIN_ID,
                "ngrok connections did not end within 5 s of the transport drop"
            );
        }
    }

    fn transport_handle_listen_address(&self, transport_handle: WatchHandleBox) -> Option<String> {
        // SAFETY: as in `transport_handle_close`.
        let endpoint = unsafe { &*(transport_handle.0 as *const Endpoint) };
        Some(endpoint.url.clone())
    }
}

/// Open the ngrok session and the HTTP endpoint.
async fn open(config: &NgrokConfig) -> Result<(Session, HttpTunnel), String> {
    let mut builder = Session::builder();
    builder.authtoken(config.authtoken.clone()).client_info(
        "mcpg-gateway",
        env!("CARGO_PKG_VERSION"),
        None::<String>,
    );
    if let Some(addr) = &config.server_addr {
        builder
            .server_addr(addr.clone())
            .map_err(|e| format!("ngrok server_addr: {e}"))?;
    }
    let session = builder
        .connect()
        .await
        .map_err(|e| format!("ngrok session: {e}"))?;
    let mut endpoint = session.http_endpoint();
    endpoint.pooling_enabled(config.pooling).forwards_to(
        config
            .forwards_to
            .clone()
            .unwrap_or_else(|| "mcpg gateway".into()),
    );
    if let Some(domain) = &config.domain {
        endpoint.domain(domain.clone());
    }
    if let Some(policy) = &config.traffic_policy {
        endpoint.traffic_policy(policy.clone());
    }
    if let Some(binding) = &config.binding {
        endpoint.binding(binding.clone());
    }
    if let Some(metadata) = &config.metadata {
        endpoint.metadata(metadata.clone());
    }
    for cidr in &config.allow_cidrs {
        endpoint.allow_cidr(cidr.clone());
    }
    for cidr in &config.deny_cidrs {
        endpoint.deny_cidr(cidr.clone());
    }
    let tunnel = endpoint
        .listen()
        .await
        .map_err(|e| format!("ngrok endpoint: {e}"))?;
    Ok((session, tunnel))
}

/// Accept connections until `stop`, serving each one until it closes or
/// `kill`.
async fn accept_loop(
    mut session: Session,
    mut tunnel: HttpTunnel,
    server: HttpIngressServer,
    stop: CancellationToken,
    kill: CancellationToken,
    tasks: TaskTracker,
    max_body: usize,
) {
    let mut retry = ACCEPT_RETRY;
    loop {
        let next = tokio::select! {
            () = stop.cancelled() => break,
            next = tunnel.next() => next,
        };
        match next {
            Some(Ok(conn)) => {
                retry = ACCEPT_RETRY;
                let remote = conn.remote_addr();
                let kill = kill.clone();
                tasks.spawn(async move {
                    tokio::select! {
                        () = serve_connection(
                            conn,
                            server,
                            Connection::from_addr(remote, max_body),
                            "ngrok",
                        ) => {}
                        () = kill.cancelled() => {}
                    }
                });
            }
            Some(Err(e)) => {
                tracing::warn!(
                    plugin_id = PLUGIN_ID,
                    error = %e,
                    retry_ms = retry.as_millis(),
                    "ngrok accept failed"
                );
                tokio::select! {
                    () = stop.cancelled() => break,
                    () = tokio::time::sleep(retry) => {}
                }
                retry = (retry * 2).min(ACCEPT_RETRY_MAX);
            }
            None => {
                tracing::warn!(plugin_id = PLUGIN_ID, "the ngrok endpoint closed");
                break;
            }
        }
    }
    if let Err(e) = tunnel.close().await {
        tracing::debug!(plugin_id = PLUGIN_ID, error = %e, "ngrok endpoint close failed");
    }
    if let Err(e) = session.close().await {
        tracing::debug!(plugin_id = PLUGIN_ID, error = %e, "ngrok session close failed");
    }
}

/// `text` with every copy of `secret` replaced.
fn redact(text: &str, secret: &str) -> String {
    if secret.is_empty() {
        text.to_owned()
    } else {
        text.replace(secret, "<redacted>")
    }
}

declare_plugin! {
    plugin_id: PLUGIN_ID,
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[
        mcpg_plugin_protocol::capability::Capability::NetworkOutbound,
        mcpg_plugin_protocol::capability::Capability::TransportListen,
    ],
    entities: [
        transport as entity {
            inner_name: "",
            plugin_type: NgrokTransport,
            factory: |cfg: &str, _host: ::mcpg_plugin_sdk::HostHandle| -> NgrokTransport {
                NgrokTransport::new(cfg)
            },
        }
    ],
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_transports_entry_config_wins_and_the_plugin_config_fills_an_empty_one() {
        let plugin = NgrokTransport::new(r#"{"authtoken":"from-plugin"}"#);
        let own = plugin
            .config_for(&json!({"authtoken": "from-transport"}))
            .unwrap();
        assert_eq!(own.authtoken, "from-transport");
        for empty in [serde_json::Value::Null, json!({})] {
            assert_eq!(plugin.config_for(&empty).unwrap().authtoken, "from-plugin");
        }
    }

    #[test]
    fn it_is_an_http_wire_transport_and_refuses_the_messages_wire() {
        let plugin = NgrokTransport::new("{}");
        assert_eq!(plugin.wire(), TransportWire::Http);
        let dispatcher: std::sync::Arc<dyn SyncMessageDispatcher> = std::sync::Arc::new(NoDispatch);
        let err = plugin.start(&json!({}), dispatcher).err().unwrap();
        assert!(matches!(err, TransportError::InvalidConfig { .. }));
    }

    #[test]
    fn a_bad_config_is_refused_before_any_network_call() {
        let plugin = NgrokTransport::new("{}");
        let server = HttpIngressServer::from_ffi(mcpg_plugin_protocol::abi::HttpServerRef {
            ctx: 0,
            serve: never_serve,
            cancel: never_cancel,
        });
        let err = plugin.start_http(&json!({}), server).err().unwrap();
        let TransportError::InvalidConfig { message } = err else {
            panic!("expected InvalidConfig");
        };
        assert!(message.contains("authtoken"), "{message}");
    }

    #[test]
    fn an_error_text_never_carries_the_authtoken() {
        assert_eq!(
            redact("bad token 2abc: ERR_NGROK_105", "2abc"),
            "bad token <redacted>: ERR_NGROK_105"
        );
        assert_eq!(redact("x", ""), "x");
    }

    struct NoDispatch;
    impl SyncMessageDispatcher for NoDispatch {
        fn dispatch(
            &self,
            _session_id: &str,
            _message: &[u8],
        ) -> Result<Vec<u8>, mcpg_plugin_protocol::transport::DispatcherError> {
            unreachable!()
        }
    }

    extern "C" fn never_serve(
        _: usize,
        _: mcpg_plugin_sdk::abi_stable::std_types::RString,
        _: mcpg_plugin_sdk::abi_stable::std_types::RVec<u8>,
        _: mcpg_plugin_protocol::abi::HttpResponderRef,
    ) -> u64 {
        unreachable!()
    }

    extern "C" fn never_cancel(_: usize, _: u64) {}
}
