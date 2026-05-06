# Hestia Assistant Networking Notes

The phone-side assistant is split across local shell services and remote AI/voice
backends. For the full deployment, the device and backend machines need to be
able to reach each other across Tailscale.

## Required paths

1. Phone -> Unmute realtime backend
   - Used by `unmute-streaming-client` / future voice daemon.
   - WebSocket endpoint: `ws://<tailscale-node>:<port>/v1/realtime` or TLS equivalent.

2. Phone -> agentic/orchestrator backend
   - Used by `hestia-ai-bridge` via `ORCHESTRATOR_URL`.
   - HTTP/SSE endpoint should bind on the backend node's Tailscale interface or a
     localhost reverse proxy exposed to Tailscale.

3. Local-only phone sockets
   - `AI_SOCKET_PATH`, default: `$XDG_RUNTIME_DIR/hestia-shell/ai.sock`
   - `ASSISTANT_SOCKET_PATH`, default: `$XDG_RUNTIME_DIR/hestia-shell/assistant.sock`
   - These are Unix sockets on the phone and should not be exposed over Tailscale.

## Tailscale ACL direction

At minimum, allow the phone node/tag to connect to the backend node/tag on the
Unmute realtime port and orchestrator port. Keep access directional if possible:

```jsonc
{
  "acls": [
    {
      "action": "accept",
      "src": ["tag:hestia-phone"],
      "dst": [
        "tag:hestia-backend:<unmute-port>",
        "tag:hestia-backend:<orchestrator-port>"
      ]
    }
  ]
}
```

If MagicDNS is enabled, prefer stable names like
`http://hestia-backend.<tailnet>.ts.net:<port>` in environment/config instead of
hard-coded 100.x addresses.

## Service env knobs

```bash
ORCHESTRATOR_URL=http://<backend-tailnet-name>:<orchestrator-port>
AI_SOCKET_PATH=$XDG_RUNTIME_DIR/hestia-shell/ai.sock
ASSISTANT_SOCKET_PATH=$XDG_RUNTIME_DIR/hestia-shell/assistant.sock
```

For the current phone voice client:

```bash
python headless_client_microphone.py \
  --no-tui \
  --server-url ws://<backend-tailnet-name>:<unmute-port> \
  --assistant-socket "$XDG_RUNTIME_DIR/hestia-shell/assistant.sock" \
  --input-device 11 \
  --output-device 11 \
  --verbose
```

Pass `--assistant-socket ''` to run the test client without publishing orb/chat
events to the local shell. For `tiny-emerson`, use `ws://tiny-emerson:80` as the
base server URL; the client appends `/v1/realtime` internally.

## Current tiny-emerson deployment

Verified phone/backend paths:

```text
agentic_flow orchestrator: http://tiny-emerson:8000/health
Unmute backend health:    http://tiny-emerson/v1/health
Unmute realtime WS:       ws://tiny-emerson:80/v1/realtime
```

`http://tiny-emerson/health` is not the Unmute health endpoint and may return
404. A plain HTTP GET to `/v1/realtime` may also return 404; use a WebSocket
upgrade probe to verify realtime readiness.

The phone-side durable voice service is documented in the
`unmute-streaming-client` repo at:

```text
docs/hestia-phone-voice-service.md
systemd/hestia-unmute-voice.service
```
