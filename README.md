# ngrok Transport (`dev.mcpg.transport.ngrok`)

A **transport** plugin that serves the gateway at an
[ngrok](https://ngrok.com) endpoint. The plugin opens an outbound ngrok
session, so the gateway needs no inbound port and no public IP.

It is an `http`-wire transport: it does not parse MCP. It hands every HTTP
request it receives to the gateway's own HTTP router. These features
therefore work as on the gateway's HTTP listener:

- identity and OAuth discovery (the `Host` header is the ngrok domain),
- per-IP rate limits, keyed on the client address that ngrok reports,
- SSE streams,
- both MCP revisions (`2025-11-25` and `2026-07-28`).

## How a request flows

1. ngrok's edge receives the client's HTTPS request and runs the endpoint's
   Traffic Policy, if you set one.
2. The plugin reads the request from the ngrok session, with its body up
   to `max_request_body_bytes`. A larger body gets HTTP 413.
3. The gateway router answers. The plugin streams the answer back frame by
   frame. When the client goes away, the gateway stops the response.

The ngrok session reconnects by itself after a network failure and binds
the endpoint again. The endpoint URL is the transport's listen address. The
gateway also uses it as its public address for webhook callbacks, unless
`gateway.server.canonical_url` is set.

## Configuration

Put the keys in the `config:` of the `gateway.server.transports[]` entry.
If that entry has no `config:`, the plugin uses the `config:` of its
`plugins[]` entry.

| Field | Type | Default | Description |
|---|---|---|---|
| `authtoken` | string | *(required)* | The ngrok authtoken. Set it from `${env.NGROK_AUTHTOKEN}`. |
| `domain` | string | ngrok's dev domain | A reserved domain such as `tools.example.ngrok.app`, or `tools.internal` with `binding: internal`. |
| `traffic_policy` | string or map | *(none)* | An ngrok Traffic Policy that runs at ngrok's edge: a YAML or JSON string, or the policy as a map. |
| `binding` | string | ngrok's default | `public`, `internal` or `kubernetes`. |
| `pooling` | bool | `false` | Lets several gateways serve the same domain. ngrok balances between them. |
| `metadata` | string | *(none)* | Endpoint metadata for the ngrok dashboard and API. |
| `forwards_to` | string | `mcpg gateway` | The `forwards_to` label in the ngrok dashboard. |
| `allow_cidrs` | list | `[]` | Source CIDRs that ngrok lets through. ngrok refuses all others. |
| `deny_cidrs` | list | `[]` | Source CIDRs that ngrok refuses. |
| `server_addr` | string | ngrok's default | The ngrok session server, as `host:port`. |
| `connect_timeout_secs` | int | `30` | How long to wait for the session and the endpoint at boot. |
| `max_request_body_bytes` | int | `4194304` | The largest request body that the plugin reads. |

```yaml
plugins:
  - id: dev.mcpg.transport.ngrok
    kind: native
    class: transport
    source:
      oci: ghcr.io/mcpg-dev/plugins/transport-ngrok
    granted_capabilities:
      - network_outbound
      - transport_listen

gateway:
  server:
    transports:
      - kind: dev.mcpg.transport.ngrok
        config:
          authtoken: ${env.NGROK_AUTHTOKEN}
          domain: tools.example.ngrok.app
          traffic_policy: |
            on_http_request:
              - expressions: ["!('com.anthropic.api' in conn.client_ip.categories)"]
                actions: [{ type: deny, config: { status_code: 403 } }]
```

If the session or the endpoint does not open within
`connect_timeout_secs`, the gateway does not start. The error names the
cause, for example a refused authtoken or a domain that the account has
not reserved.

## Security notes

- ngrok terminates TLS at its edge, so ngrok sees the traffic in
  plaintext. Use the gateway's own identity and policy for access control.
  A Traffic Policy is an additional layer at the edge.
- The authtoken never appears in logs or in error text.
- A pooled endpoint sends a client's requests to any gateway in the pool.
  On the `2025-11-25` wire, MCP sessions then need a shared session store
  (cluster mode), or the clients must use the stateless `2026-07-28`
  revision.

## License

Apache-2.0 — see [LICENSE](LICENSE).
