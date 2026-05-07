# hestia-ai-bridge

Local protocol adapter between the hestia shell's `ai.sock` and the
`agentic_flow` orchestrator. Runs on the shell host next to emerson.

```
┌─────────────────┐       ┌───────────────────┐       ┌─────────────────┐
│  hestia-shell   │─UDS──▶│  hestia-ai-bridge │──HTTP▶│  orchestrator   │
│  (ai.sock)      │       │   (this project)  │       │   (:8000)       │
└─────────────────┘       └─────────┬─────────┘       └─────────────────┘
                                    │ UDS
                                    ▼
                          ┌───────────────────┐
                          │      emerson      │
                          │  (/tmp/emerson)   │
                          └───────────────────┘
```

Two responsibilities:

1. **Chat proxy** — accept `chat` requests on `ai.sock` (newline-delimited
   JSON, spec in `hestia-OS/ai-integration-spec.md`), forward to orchestrator
   `/v1/chat/completions`, translate SSE back into the shell's 5-type protocol
   (`token` / `tool_call` / `tool_result` / `done` / `error`).
2. **Desktop state endpoint** — `GET /desktop_state` aggregates live emerson
   snapshots (active window, workspaces, notifications, clipboard, system,
   workstates) for consumption by the orchestrator's LSA. `GET /health` for
   monitoring.

See `CLAUDE.md` in this repo for details on the design decisions behind
the scope cuts (why the bridge is a thin adapter, not an orchestration
layer).

## Install

```bash
cd ~/Desktop/hestia-ai-bridge
python3 -m venv .venv
.venv/bin/pip install -e .

mkdir -p ~/.config/systemd/user
cp systemd/hestia-ai-bridge.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now hestia-ai-bridge
```

## Configuration

All via environment variables. Override in a systemd drop-in
(`systemctl --user edit hestia-ai-bridge`):

| Variable                 | Default                                | Meaning                               |
|--------------------------|----------------------------------------|---------------------------------------|
| `ORCHESTRATOR_URL`       | `http://localhost:8000`                | agentic_flow orchestrator base URL    |
| `AI_SOCKET_PATH`         | `/run/user/$UID/hestia-shell/ai.sock`  | Shell-facing UDS                      |
| `BRIDGE_HTTP_HOST`       | `127.0.0.1`                            | HTTP bind host for `/desktop_state`   |
| `BRIDGE_HTTP_PORT`       | `8765`                                 | HTTP bind port                        |
| `BRIDGE_TOKEN`           | *(none — open)*                        | If set, require `Bearer <token>`      |
| `EMERSON_SOCKET`         | `/tmp/emerson.sock`                    | Emerson daemon UDS                    |
| `SESSION_STATE_PATH`     | `$XDG_STATE_HOME/hestia-ai-bridge/session.json` | Persistent session_id store |
| `ORCH_HEALTH_INTERVAL`   | `30`                                   | Seconds between health probes (online)|
| `ORCH_RETRY_INITIAL`     | `5`                                    | Offline backoff start (seconds)       |
| `ORCH_RETRY_MAX`         | `300`                                  | Offline backoff cap (seconds)         |
| `ORCH_FAILURE_THRESHOLD` | `3`                                    | Consecutive failed probes before marking orchestrator offline |
| `EMERSON_POLL_TIMEOUT`   | `2.0`                                  | Per-IPC timeout to emerson            |
| `LOG_LEVEL`              | `INFO`                                 | DEBUG / INFO / WARNING / ERROR        |

## Endpoints

### `GET /health`

Always open. Returns bridge status, orchestrator online/offline, timestamp,
and sanitized orchestrator health metadata. It intentionally omits the
orchestrator URL and raw upstream error details because `/health` bypasses
`BRIDGE_TOKEN` for monitoring compatibility.

```json
{ "status": "ok", "orchestrator_online": true, "ts": "2026-04-20T..." }
```

The nested `orchestrator.status` is `unknown` until the first async probe
completes after bridge startup, `online` after a successful probe, `degraded`
while transient failures are below `ORCH_FAILURE_THRESHOLD`, and `offline`
after the threshold is reached.

### `GET /desktop_state`

Requires `Authorization: Bearer <BRIDGE_TOKEN>` if configured. Aggregates:

```json
{
  "ts": "...",
  "desktop":            { ... emerson event_listener state ... },
  "active_window":      { "address": "...", "title": "...", "class": "..." },
  "notifications":      [ ... ],
  "clipboard_preview":  "first 500 chars",
  "clipboard_truncated": false,
  "system":             { battery, bluetooth, network, audio, ... },
  "workstates":         [ { "name": "...", "description": "..." }, ... ],
  "current_workstate":  "...",
  "elapsed_ms":         3,
  "errors":             { "clipboard": "..." }
}
```

If every emerson call fails, `stale: true` is set and the response returns
HTTP 503 so LSA can cleanly skip the assessment.

### `GET /mobile_capabilities`

Requires `Authorization: Bearer <BRIDGE_TOKEN>` if configured. Returns the
sanitized, local-only phone interface contract an agent/orchestrator can use to
discover supported Hestia Mobile visual verbs, assistant states, protected modes,
and socket paths. It intentionally reports only phone-local IPC surfaces; do not
expose `ai.sock` or `assistant.sock` over Tailscale.

```json
{
  "interface": "hestia-mobile-agent-phone-interface",
  "transport": "local-only",
  "orchestrator_online": true,
  "sockets": {
    "ai": "/run/user/1000/hestia-shell/ai.sock",
    "assistant": "/run/user/1000/hestia-shell/assistant.sock"
  },
  "visual_verbs": ["show_card", "update_card", "dismiss_card"],
  "protected_modes": ["phone_call_active", "offline", "error"]
}
```

The nested `orchestrator` object uses the same sanitized metadata policy as
`/health`: no raw orchestrator URL, secrets, or upstream error details.

## Protocol translation (chat path)

Inbound on `ai.sock` (per `hestia-OS/ai-integration-spec.md` §1):
```
{"type": "chat", "model": "...", "messages": [...], "tools": [...]}\n
```

Outbound frames (one per line):
- `{"type": "token", "content": "..."}` — assistant text delta
- `{"type": "tool_call", "name": "...", "args": {...}}` — tool invoked
- `{"type": "tool_result", "name": "", "result": ""}` — synthetic, emitted
  when content resumes after a tool_call (orchestrator currently suppresses
  tool_result in SSE; this is an MVP inference)
- `{"type": "done"}` — generation finished
- `{"type": "error", "message": "..."}` — terminal error

Every request is terminated by exactly one `done` or `error`.

## Follow-ups

- **Explicit `tool_result` events** — orchestrator intentionally suppresses
  `tool_result` from SSE (crashes OpenAI SDK clients). Adding a custom
  `agent.tool_result` event in `_stream_with_loop` would let the bridge
  stop inferring tool completion.
- **Emerson event subscription** — v1 polls emerson per request. When
  emerson grows an external event-stream API, the bridge should subscribe
  and maintain a live cached snapshot. Enables SSE `/desktop_state/events`.
- **LSA rewiring** — `agentic_flow/orchestrator/live_state_assessor.py`
  should call `GET /desktop_state` on the bridge instead of pulling
  emerson MCP directly (~Tailscale latency savings when orchestrator is
  remote). Keep `emerson_mcp_client.py` for imperative actions.
- **Prefill context envelope** — the shell's right-click → prefill path
  (gaps.md H1) currently sends plain text. Extending it to include
  `{notif_id, app_name, ...}` in an `extra_context` field lets the
  manager act on specific objects; the bridge already forwards non-spec
  fields via `extra_context` when present.
