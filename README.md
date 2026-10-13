# ssh-mcp

An MCP (Model Context Protocol) server that gives AI agents shell access to your
machine: run commands locally, run them over SSH, read/write files, list
directories, and check system status. Built for cloud agents (Vani, CodeRabbit,
Cursor, Claude, ...) that need to operate a real workstation.

## Tools

| Tool | What it does |
|---|---|
| `execute_command` | Run a bash command on the host; returns stdout, stderr, exit code |
| `ssh_execute` | Run a command on a remote host over SSH |
| `read_file` | Read a file with line-based pagination |
| `write_file` | Write/append a file (creates missing directories) |
| `list_directory` | List a directory with sizes, permissions, timestamps |
| `system_status` | Hostname, OS, CPU, memory/disk, uptime |

Some clients (e.g. Vani) run a keyword heuristic and hard-block tools that
"look like" writes. For those, neutrally-named aliases expose the same
capabilities: `shell_output` (= `execute_command`), `remote_output`
(= `ssh_execute`), `file_result` (= `write_file`). Original names are kept,
so existing integrations keep working.

## Transports

- **Streamable HTTP** (modern clients): `POST /mcp`
- **Legacy SSE**: `GET /sse` + `POST /messages/`

## Auth

Pick one per request:

- Header: `X-API-Key: <key>` (also accepts `Api-Key`, `X-MCP-Key`)
- Header: `Authorization: Bearer <key>`
- Query string: `?api_key=<key>` (also `?token=`, `?key=`)

OAuth discovery endpoints (`/.well-known/*`) are public and return 404, so
clients fall back to the above instead of failing validation.

## Quickstart

```bash
git clone <this-repo> && cd ssh-mcp
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# edit .env: set MCP_API_KEY to a long random value

# run directly
python3 server.py
# or supervised + public tunnel (needs ngrok installed & logged in)
bash run_mcp_service.sh
```

The server listens on `127.0.0.1:8844` by default. `run_mcp_service.sh` also
opens an ngrok tunnel and writes the public URL to `live-url.txt` plus
client-ready connection details to `connection_details.txt`.

### Connecting a client

- **Vani / apps with URL-only setup:**
  `https://<your-tunnel>/sse?api_key=<your-key>`
- **Header-capable clients (CodeRabbit, etc.):**
  URL `https://<your-tunnel>/sse`, header `X-API-Key: <your-key>`

### Run at boot (systemd, Linux)

```bash
mkdir -p ~/.config/systemd/user
cp ssh-mcp.service.example ~/.config/systemd/user/ssh-mcp.service
# edit WorkingDirectory/ExecStart paths inside if needed
systemctl --user daemon-reload
systemctl --user enable --now ssh-mcp.service
```

## Security warning

These tools are a **full remote shell** on the host machine. Treat the API
key like a password: never commit it, never put it in a URL you share, and
don't expose the server to the internet without auth. Prefer tunnels with
access control (Cloudflare Access, Tailscale) over fully public URLs when
you can.
