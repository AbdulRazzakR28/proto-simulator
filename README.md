# MQTT Protobuf Decoder — Internal Dashboard

A secure, internal MQTTX-style web tool that connects to your MQTT broker,
auto-decodes Protobuf payloads from all configured topics, and presents them
in a live, searchable browser UI.

---

## Files

```
mqttx_decoder/
├── app.py            ← Flask server (main entry point)
├── mqtt_pb2.py       ← Compiled Protobuf (generated from mqtt.proto)
├── mqtt.proto        ← Source proto definition (reference only)
├── requirements.txt  ← Python dependencies
├── README.md
└── templates/
    ├── login.html    ← Password-protected login page
    └── dashboard.html ← Main MQTTX-style dashboard
```

---

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. (Optional) Change the password in app.py
#    Edit the ACCESS_PASSWORD variable near the top of app.py

# 3. Run
python3 app.py

# 4. Open in browser
open http://localhost:5000
```

Default password: `decoder2025`  ← **Change this before deploying!**

---

## Security

- All routes return 401 unless the session is authenticated.
- All WebSocket events are rejected for unauthenticated connections.
- Password is SHA-256 hashed in memory; never stored or logged.
- For production: set `SECRET_KEY` in `app.py` to a long random string,
  run behind an HTTPS reverse proxy (nginx/caddy), and restrict access
  by IP or VPN.

---

## Features

| Feature | Description |
|---|---|
| Live MQTT subscription | Connects to any broker; subscribes to `#` (all topics) |
| Auto protobuf decode | Matches each topic to `TOPIC_MAP`; decodes Protobuf automatically |
| MAC address formatting | `beacon_mac`, `device_mac`, `bssid` etc. shown as `AA:BB:CC:DD:EE:FF` |
| Decoded JSON view | Syntax-coloured, readable JSON for every message |
| Raw Base64 view | Original payload visible for verification |
| Message metadata | QoS, retained, payload size, timestamp, message type |
| Supported topics tab | Browse all configured topic patterns and their Protobuf types |
| Manual decode | Paste any topic + Base64 string without live MQTT connection |
| Login protection | Simple password gate; session-based auth |
| In-memory ring buffer | Keeps last 50 messages per topic |

---

## Adding new topics

Edit the `_build_topic_map()` function in `app.py`:

```python
{"pattern": r"^dt/.+/sensor/newtype$", "type": mqtt_pb2.MyNewMessage,
 "desc": "My new sensor", "qos": 0, "retained": False},
```

That's it — no other changes needed.

---

## Regenerating mqtt_pb2.py

If you update `mqtt.proto`:

```bash
pip install grpcio-tools
python -m grpc_tools.protoc -I. --python_out=. mqtt.proto
```

---

## Production deployment (nginx example)

```nginx
server {
    listen 443 ssl;
    server_name mqtt-decoder.internal;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
    }
}
```

Run the app with gunicorn + eventlet:
```bash
pip install gunicorn eventlet
gunicorn -k eventlet -w 1 app:app --bind 0.0.0.0:5000
```
