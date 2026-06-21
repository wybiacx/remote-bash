# remote-bash

MCP Server that lets AI agents execute arbitrary bash commands on the remote machine via SSE over HTTP.

## Quick Start

```bash
# Build
cd remote-bash
cargo build --release

# Run (HTTP mode)
export MCP_TOKEN="your-secret-token"
./target/release/remote-bash

# Run (HTTPS mode, self-signed cert auto-generated)
export MCP_TOKEN="your-secret-token"
export MCP_TLS=true
./target/release/remote-bash
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MCP_TOKEN` | Bearer token for authentication (required) | — |
| `MCP_PORT` | Listen port | `9020` |
| `MCP_TLS` | Enable TLS (`true` / `1`) | `false` |
| `MCP_TLS_CERT` | Custom certificate PEM path | Auto-generated |
| `MCP_TLS_KEY` | Custom private key PEM path | Auto-generated |

With TLS enabled, a self-signed certificate is auto-generated in `~/.remote-bash/`. The server prints the SHA-256 fingerprint at startup, which the client can use for certificate pinning (`cert_sha256`).

## MCP Client Configuration

```json
{
  "mcpServers": {
    "remote-bash": {
      "url": "https://localhost:9020/sse",
      "headers": {
        "Authorization": "Bearer your-secret-token"
      },
      "cert_sha256": "9bf69b32622ef2e8cfdb28600debf4ec375019e39ea07dbf592d8608c785600e"
    }
  }
}
```

For plain HTTP, change `url` to `http://` and omit `cert_sha256`.

## Available Tool

### `execute_command`

Executes an arbitrary bash command.

| Parameter | Type | Description |
|-----------|------|-------------|
| `command` | string | The command to execute (required) |
| `timeout` | integer | Timeout in seconds, default 30 |

## Architecture

```
Client (AI Agent)              remote-bash (MCP Server)
     │                                │
     ├── GET /sse ────────────────────┤  Establish SSE stream, obtain session_id
     │   Authorization: Bearer <token> │
     │                                │
     ├── POST /messages?session_id=..  │  Send JSON-RPC request
     │   ← SSE event: message ────────┤  Response pushed via SSE
```

- Transport: SSE + HTTP POST, JSON-RPC 2.0 protocol
- Auth: Bearer Token (`Authorization` header)
- Encryption: Optional TLS (`MCP_TLS=true`)
- Concurrency: Multiple sessions share the same OS user, no resource isolation

## License

MIT
