//! Serve HTTP on one client connection and hand each request to the
//! gateway.
//!
//! The connection is any byte stream (an ngrok endpoint connection here);
//! hyper reads HTTP/1.1 or HTTP/2 from it. Each request body is read whole
//! (MCP request bodies are small JSON documents), handed to the gateway,
//! and the gateway's answer is streamed back frame by frame, so SSE works.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{HeaderName, HeaderValue, Request, Response, StatusCode};
use http_body::{Body, Frame, SizeHint};
use http_body_util::{BodyExt as _, Limited};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use mcpg_plugin_protocol::transport::HttpIngressRequestHead;
use mcpg_plugin_sdk::http_ingress::{HttpIngressServer, IngressResponse};
use tokio::io::{AsyncRead, AsyncWrite};

/// Connection-specific headers HTTP/2 refuses and HTTP/1.1 sets itself.
const HOP_BY_HOP: [&str; 6] = [
    "connection",
    "keep-alive",
    "proxy-connection",
    "transfer-encoding",
    "upgrade",
    "te",
];

/// Serve one client connection until it closes.
pub async fn serve_connection<I>(
    io: I,
    server: HttpIngressServer,
    remote_addr: SocketAddr,
    max_body: usize,
) where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = hyper::service::service_fn(move |request: Request<Incoming>| async move {
        Ok::<_, Infallible>(answer(request, server, remote_addr, max_body).await)
    });
    let builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    if let Err(e) = builder.serve_connection(TokioIo::new(io), service).await {
        tracing::debug!(error = %e, remote = %remote_addr, "ngrok connection ended with an error");
    }
}

async fn answer(
    request: Request<Incoming>,
    server: HttpIngressServer,
    remote_addr: SocketAddr,
    max_body: usize,
) -> Response<ResponseBody> {
    let (parts, body) = request.into_parts();
    let head = match request_head(&parts, remote_addr) {
        Ok(head) => head,
        Err(reason) => return plain(StatusCode::BAD_REQUEST, reason),
    };
    let body = match Limited::new(body, max_body).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e)
            if e.downcast_ref::<http_body_util::LengthLimitError>()
                .is_some() =>
        {
            return plain(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request body too large".into(),
            );
        }
        Err(_) => return plain(StatusCode::BAD_REQUEST, "request body unreadable".into()),
    };
    let mut response = server.serve(&head, body.to_vec());
    let head = match response.head().await {
        Ok(head) => head,
        Err(reason) => {
            tracing::warn!(%reason, "the gateway produced no response");
            return plain(
                StatusCode::BAD_GATEWAY,
                "the gateway produced no response".into(),
            );
        }
    };
    let mut builder = Response::builder().status(head.status);
    for (name, value) in &head.headers {
        if HOP_BY_HOP.contains(&name.to_ascii_lowercase().as_str()) {
            continue;
        }
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            builder = builder.header(name, value);
        }
    }
    builder
        .body(ResponseBody::Gateway(response))
        .unwrap_or_else(|_| {
            plain(
                StatusCode::BAD_GATEWAY,
                "the gateway sent an invalid head".into(),
            )
        })
}

/// The gateway's view of the request. HTTP/2 carries the host in the
/// `:authority` pseudo-header; it becomes `Host`, which the gateway's
/// OAuth discovery reads.
fn request_head(
    parts: &http::request::Parts,
    remote_addr: SocketAddr,
) -> Result<HttpIngressRequestHead, String> {
    let mut headers = Vec::with_capacity(parts.headers.len() + 1);
    for (name, value) in &parts.headers {
        let value = value
            .to_str()
            .map_err(|_| format!("header {name} is not visible ASCII"))?;
        headers.push((name.as_str().to_owned(), value.to_owned()));
    }
    if !parts.headers.contains_key(http::header::HOST)
        && let Some(authority) = parts.uri.authority()
    {
        headers.push(("host".to_owned(), authority.as_str().to_owned()));
    }
    let uri = parts
        .uri
        .path_and_query()
        .map_or_else(|| "/".to_owned(), |pq| pq.as_str().to_owned());
    Ok(HttpIngressRequestHead {
        method: parts.method.as_str().to_owned(),
        uri,
        headers,
        remote_addr: Some(remote_addr.to_string()),
        tls: None,
    })
}

fn plain(status: StatusCode, message: String) -> Response<ResponseBody> {
    let mut response = Response::new(ResponseBody::Plain(Some(Bytes::from(message))));
    *response.status_mut() = status;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// A response body: the gateway's streamed answer, or a short local one.
/// Dropping a `Gateway` body before it ends tells the gateway the client
/// went away.
pub enum ResponseBody {
    Gateway(IngressResponse),
    Plain(Option<Bytes>),
}

/// The gateway stopped a response early.
#[derive(Debug)]
pub struct StoppedByGateway(pub String);

impl std::fmt::Display for StoppedByGateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the gateway stopped the response: {}", self.0)
    }
}

impl std::error::Error for StoppedByGateway {}

impl Body for ResponseBody {
    type Data = Bytes;
    type Error = StoppedByGateway;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, StoppedByGateway>>> {
        match self.get_mut() {
            Self::Plain(data) => Poll::Ready(data.take().map(|d| Ok(Frame::data(d)))),
            Self::Gateway(response) => response.poll_chunk(cx).map(|chunk| {
                chunk.map(|frame| {
                    frame
                        .map(|data| Frame::data(Bytes::from(data)))
                        .map_err(StoppedByGateway)
                })
            }),
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self, Self::Plain(None))
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Plain(Some(data)) => SizeHint::with_exact(data.len() as u64),
            Self::Plain(None) => SizeHint::with_exact(0),
            Self::Gateway(_) => SizeHint::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Full;
    use mcpg_plugin_protocol::abi::{HTTP_SINK_ACCEPTED, HttpResponderRef, HttpServerRef};
    use mcpg_plugin_protocol::transport::HttpIngressResponseHead;
    use mcpg_plugin_sdk::abi_stable::std_types::{RString, RVec};
    use std::sync::Mutex;
    use std::time::Duration;

    /// Requests the fake gateway saw.
    static SEEN: Mutex<Vec<HttpIngressRequestHead>> = Mutex::new(Vec::new());
    /// Responders of `/hold` requests, kept open until a test ends them.
    static HELD: Mutex<Vec<(u64, HttpResponderRef)>> = Mutex::new(Vec::new());
    static CANCELLED: Mutex<Vec<u64>> = Mutex::new(Vec::new());
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    /// The tests share the statics above; one at a time.
    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn send_head(r: &HttpResponderRef, status: u16, headers: Vec<(String, String)>) {
        let head = HttpIngressResponseHead { status, headers };
        assert_eq!(
            (r.head)(r.ctx, RString::from(serde_json::to_string(&head).unwrap())),
            HTTP_SINK_ACCEPTED
        );
    }

    /// The fake gateway: `/echo` answers with the body, `/sse` with two
    /// events, `/hold` with a head and an open body, `/fail` with no head.
    extern "C" fn fake_serve(
        _ctx: usize,
        head_json: RString,
        body: RVec<u8>,
        r: HttpResponderRef,
    ) -> u64 {
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let head: HttpIngressRequestHead = serde_json::from_str(head_json.as_str()).unwrap();
        let path = head.uri.split('?').next().unwrap_or_default().to_owned();
        SEEN.lock().unwrap().push(head);
        match path.as_str() {
            "/echo" => {
                send_head(
                    &r,
                    200,
                    vec![
                        ("content-type".into(), "application/json".into()),
                        ("connection".into(), "keep-alive".into()),
                    ],
                );
                (r.chunk)(r.ctx, body);
                (r.end)(r.ctx, RString::new());
            }
            "/sse" => {
                send_head(
                    &r,
                    200,
                    vec![("content-type".into(), "text/event-stream".into())],
                );
                (r.chunk)(r.ctx, RVec::from(b"data: a\n\n".to_vec()));
                (r.chunk)(r.ctx, RVec::from(b"data: b\n\n".to_vec()));
                (r.end)(r.ctx, RString::new());
            }
            "/hold" => {
                send_head(
                    &r,
                    200,
                    vec![("content-type".into(), "text/event-stream".into())],
                );
                (r.chunk)(r.ctx, RVec::from(b"data: first\n\n".to_vec()));
                HELD.lock().unwrap().push((id, r));
            }
            _ => (r.end)(r.ctx, RString::from("no route")),
        }
        id
    }

    extern "C" fn fake_cancel(_ctx: usize, id: u64) {
        CANCELLED.lock().unwrap().push(id);
        let mut held = HELD.lock().unwrap();
        if let Some(pos) = held.iter().position(|(held_id, _)| *held_id == id) {
            let (_, r) = held.remove(pos);
            (r.end)(r.ctx, RString::from("cancelled"));
        }
    }

    fn fake_server() -> HttpIngressServer {
        HttpIngressServer::from_ffi(HttpServerRef {
            ctx: 0x1,
            serve: fake_serve,
            cancel: fake_cancel,
        })
    }

    type Sender = hyper::client::conn::http1::SendRequest<Full<Bytes>>;

    /// A served connection over an in-memory pipe, and an HTTP/1.1 client
    /// on its other end.
    async fn connect(max_body: usize) -> Sender {
        let (client, served) = tokio::io::duplex(64 * 1024);
        let remote: SocketAddr = "203.0.113.9:5151".parse().unwrap();
        tokio::spawn(serve_connection(served, fake_server(), remote, max_body));
        let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(client))
            .await
            .unwrap();
        tokio::spawn(conn);
        sender
    }

    fn post(uri: &str, body: &'static [u8]) -> Request<Full<Bytes>> {
        Request::post(uri)
            .header("host", "tools.example.ngrok.app")
            .body(Full::new(Bytes::from_static(body)))
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_goes_to_the_gateway_with_its_host_and_client_and_the_answer_comes_back() {
        let _serial = SERIAL.lock().await;
        let mut sender = connect(1024).await;
        let response = sender
            .send_request(post("/echo?x=1", b"{\"id\":1}"))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert!(
            response.headers().get("connection").is_none(),
            "hop-by-hop headers are dropped"
        );
        assert_eq!(response.headers()["content-type"], "application/json");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"{\"id\":1}");
        let seen = SEEN.lock().unwrap().last().cloned().unwrap();
        assert_eq!(seen.method, "POST");
        assert_eq!(seen.uri, "/echo?x=1");
        assert_eq!(seen.remote_addr.as_deref(), Some("203.0.113.9:5151"));
        assert!(
            seen.headers
                .contains(&("host".into(), "tools.example.ngrok.app".into()))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_sse_answer_streams_every_event() {
        let _serial = SERIAL.lock().await;
        let mut sender = connect(1024).await;
        let response = sender.send_request(post("/sse", b"")).await.unwrap();
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"data: a\n\ndata: b\n\n");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_body_over_the_limit_gets_413_and_never_reaches_the_gateway() {
        let _serial = SERIAL.lock().await;
        let before = SEEN.lock().unwrap().len();
        let mut sender = connect(4).await;
        let response = sender
            .send_request(post("/echo", b"too large"))
            .await
            .unwrap();
        assert_eq!(response.status(), 413);
        assert_eq!(SEEN.lock().unwrap().len(), before);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_gateway_answer_is_a_502() {
        let _serial = SERIAL.lock().await;
        let mut sender = connect(1024).await;
        let response = sender.send_request(post("/nowhere", b"")).await.unwrap();
        assert_eq!(response.status(), 502);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_client_that_leaves_an_open_stream_cancels_it_at_the_gateway() {
        let _serial = SERIAL.lock().await;
        let mut sender = connect(1024).await;
        let request = Request::get("/hold")
            .header("host", "h")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = sender.send_request(request).await.unwrap();
        let mut body = response.into_body();
        let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(first.as_ref(), b"data: first\n\n");
        let held_id = HELD.lock().unwrap().last().map(|(id, _)| *id).unwrap();
        drop(body);
        drop(sender);
        for _ in 0..200 {
            if CANCELLED.lock().unwrap().contains(&held_id) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the gateway never saw the cancel");
    }

    #[test]
    fn an_http2_authority_becomes_the_host_header() {
        let (parts, ()) = Request::get("https://tools.example.ngrok.app/mcp")
            .body(())
            .unwrap()
            .into_parts();
        let head = request_head(&parts, "198.51.100.1:1".parse().unwrap()).unwrap();
        assert_eq!(head.uri, "/mcp");
        assert_eq!(
            head.headers,
            vec![("host".to_owned(), "tools.example.ngrok.app".to_owned())]
        );
    }
}
