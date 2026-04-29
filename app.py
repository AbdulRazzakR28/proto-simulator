#!/usr/bin/env python3
"""
MQTT Protobuf Decoder — Internal Web Dashboard
================================================
A secure, internal MQTTX-style tool that:
  - Connects to your MQTT broker and subscribes to all configured topics
  - Auto-decodes Base64 Protobuf payloads using mqtt_pb2
  - Streams decoded messages live to a browser via WebSocket
  - Protected by a login screen (set ACCESS_PASSWORD below)

Run:
  python3 app.py
  open http://localhost:5000
"""

import base64
import hashlib
import json
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any

from flask import Flask, jsonify, render_template, request, session, send_from_directory
from flask_socketio import SocketIO, emit

# ---------------------------------------------------------------------------
# Add current directory to path so mqtt_pb2 is importable from same folder
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Auto-compile proto before import if exists
import subprocess
try:
    proto_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "protos", "mqtt.proto")
    if os.path.exists(proto_path):
        print(f"[INIT] Auto-compiling {proto_path}...")
        # Compile into the root dir so import mqtt_pb2 works
        subprocess.check_call([
            sys.executable, "-m", "grpc_tools.protoc",
            f"-I{os.path.dirname(proto_path)}",
            f"--python_out={os.path.dirname(os.path.abspath(__file__))}",
            proto_path
        ])
        print("[INIT] Protobuf compilation successful.")
except Exception as e:
    print(f"[WARN] Failed to auto-compile proto (ensure grpcio-tools is installed): {e}")

try:
    import mqtt_pb2
    from google.protobuf.json_format import MessageToDict
    PROTO_AVAILABLE = True
except ImportError as e:
    print(f"[WARN] mqtt_pb2 not found: {e}. Decoder will show raw hex only.")
    PROTO_AVAILABLE = False

try:
    import paho.mqtt.client as mqtt_client
    MQTT_AVAILABLE = True
except ImportError:
    print("[WARN] paho-mqtt not installed. Live mode disabled.")
    MQTT_AVAILABLE = False


# ===========================================================================
# ⚙  CONFIGURATION — Edit these values
# ===========================================================================

# Internal access password (loads from environment variable for security on AWS)
ACCESS_PASSWORD = os.environ.get("ACCESS_PASSWORD", "decoder2025")

# Flask session secret — randomly generated to force re-login on startup
SECRET_KEY = os.urandom(24).hex()

# How many messages to keep in the in-memory ring buffer per topic
MAX_MESSAGES_PER_TOPIC = 50

# ===========================================================================


app = Flask(__name__)
app.secret_key = SECRET_KEY
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ---------------------------------------------------------------------------
# In-memory store: { topic_string: [decoded_message_dict, ...] }
# ---------------------------------------------------------------------------
message_store: dict[str, list[dict]] = {}
store_lock = threading.Lock()

import queue
emit_queue = queue.Queue(maxsize=10000)

def ws_emitter_loop():
    while True:
        try:
            msg = emit_queue.get()
            socketio.emit("new_message", msg)
        except Exception as e:
            print(f"[WS ERROR] {e}")

threading.Thread(target=ws_emitter_loop, daemon=True).start()

# Active MQTT client handle
mqtt_handle: Any = None
mqtt_status: dict = {"connected": False, "broker": "", "error": ""}


# ===========================================================================
# Password helpers
# ===========================================================================

def _hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


HASHED_PASSWORD = _hash_pw(ACCESS_PASSWORD)


def is_authenticated() -> bool:
    return session.get("authenticated") is True


def restart_server():
    """Triggers an exit with code 3, which run.sh intercepts to restart the server."""
    print("[SERVER] Restart signal received. Exiting for clean reload...")
    # Give a small delay for the HTTP response to be sent before killing the process
    time.sleep(1.0)
    os._exit(3)


def restart_server():
    """Triggers an exit with code 3, which run.sh intercepts to restart the server."""
    print("[SERVER] Restart signal received. Exiting for clean reload...")
    # Give a small delay for the HTTP response to be sent before killing the process
    time.sleep(1.0)
    os._exit(3)


# ===========================================================================
# Protobuf Topic Map (mirrors mqtt_decoder.py — single source of truth)
# ===========================================================================

def _build_topic_map() -> list[dict[str, Any]]:
    if not PROTO_AVAILABLE:
        return []
    return [
        # ── Commands ────────────────────────────────────────────────────────
        {"pattern": r"^cmd/edge/auth/req$",    "type": mqtt_pb2.Command,          "desc": "Sign in/out command request",        "qos": 1, "retained": False},
        {"pattern": r"^cmd/edge/user/eula$",   "type": mqtt_pb2.Command,          "desc": "Set EULA version command",           "qos": 1, "retained": False},
        {"pattern": r"^cmd/edge/panic/req$",   "type": mqtt_pb2.Command,          "desc": "Trigger/dismiss panic command",      "qos": 1, "retained": False},
        {"pattern": r"^cmd/edge/user/req$",    "type": mqtt_pb2.Command,          "desc": "Set personal data (DOB) command",    "qos": 1, "retained": False},
        {"pattern": r"^cmd/edge/wrh/req$",     "type": mqtt_pb2.Command,          "desc": "Log work hours command",             "qos": 1, "retained": False},
        # ── Command Responses ───────────────────────────────────────────────
        {"pattern": r"^cmd/.+/auth/res$",      "type": mqtt_pb2.CommandResponse,  "desc": "Sign in/out command response",       "qos": 1, "retained": False},
        {"pattern": r"^cmd/.+/user/eula$",     "type": mqtt_pb2.CommandResponse,  "desc": "Set EULA version response",          "qos": 1, "retained": False},
        {"pattern": r"^cmd/.+/panic/res$",     "type": mqtt_pb2.CommandResponse,  "desc": "Panic command response",             "qos": 1, "retained": False},
        {"pattern": r"^cmd/.+/user/res$",      "type": mqtt_pb2.CommandResponse,  "desc": "Personal data command response",     "qos": 1, "retained": False},
        {"pattern": r"^cmd/.+/wrh/res$",       "type": mqtt_pb2.CommandResponse,  "desc": "Work hours command response",        "qos": 1, "retained": False},
        # ── Sensor Data ─────────────────────────────────────────────────────
        {"pattern": r"^dt/.+/sensor/beacon$",      "type": mqtt_pb2.WatchSensorData,   "desc": "Beacon sensor data",        "qos": 0, "retained": False},
        {"pattern": r"^dt/.+/sensor/heartrate$",   "type": mqtt_pb2.WatchSensorData,   "desc": "Heart rate sensor data",    "qos": 0, "retained": False},
        {"pattern": r"^dt/.+/sensor/hrv$",         "type": mqtt_pb2.WatchSensorData,   "desc": "HRV sensor data",           "qos": 0, "retained": False},
        {"pattern": r"^dt/.+/sensor/humidity$",    "type": mqtt_pb2.WatchSensorData,   "desc": "Humidity sensor data",      "qos": 0, "retained": False},
        {"pattern": r"^dt/.+/sensor/noise$",       "type": mqtt_pb2.WatchSensorData,   "desc": "Noise sensor data",         "qos": 0, "retained": False},
        {"pattern": r"^dt/.+/sensor/steps$",       "type": mqtt_pb2.WatchSensorData,   "desc": "Steps sensor data",         "qos": 0, "retained": False},
        {"pattern": r"^dt/.+/sensor/temp$",        "type": mqtt_pb2.WatchSensorData,   "desc": "Temperature sensor data",   "qos": 0, "retained": False},
        {"pattern": r"^dt/.+/sensor/heatIndex$",   "type": mqtt_pb2.WatchHeatIndexData,"desc": "Heat index sensor data",    "qos": 0, "retained": False},
        # ── Events ──────────────────────────────────────────────────────────
        {"pattern": r"^dt/.+/evt$",            "type": mqtt_pb2.WatchEventData,   "desc": "Watch event data",                   "qos": 0, "retained": False},
        # ── Metrics ─────────────────────────────────────────────────────────
        {"pattern": r"^dt/.+/metric/battery$", "type": mqtt_pb2.WatchMetricData,  "desc": "Battery metric data",                "qos": 1, "retained": False},
        {"pattern": r"^dt/.+/metric/screen$",  "type": mqtt_pb2.WatchMetricData,  "desc": "Screen metric data",                 "qos": 1, "retained": False},
        {"pattern": r"^dt/.+/metric/wifi$",    "type": mqtt_pb2.WatchMetricData,  "desc": "WiFi metric data",                   "qos": 1, "retained": False},
        # ── Edge State ──────────────────────────────────────────────────────
        {"pattern": r"^dt/edge/app/watch/version$",    "type": mqtt_pb2.AppVersionData,   "desc": "Watch app version",          "qos": 0, "retained": False},
        {"pattern": r"^dt/edge/geofence/active$",      "type": mqtt_pb2.AllGeofencesData, "desc": "Active geofences",           "qos": 0, "retained": False},
        {"pattern": r"^dt/edge/time$",                 "type": mqtt_pb2.EdgeTimeData,     "desc": "Current server time",        "qos": 0, "retained": False},
        {"pattern": r"^dt/edge/vessel/offset$",        "type": mqtt_pb2.VesselOffsetData, "desc": "Vessel UTC offset",          "qos": 0, "retained": False},
        {"pattern": r"^dt/edge/.+/user$",              "type": mqtt_pb2.WatchUserData,    "desc": "Logged-in user on watch",    "qos": 0, "retained": False},
        {"pattern": r"^dt/edge/user.+/wrh$",           "type": mqtt_pb2.WrhSummaryData,   "desc": "Work-rest hour summary",     "qos": 0, "retained": False},
        {"pattern": r"^dt/edge/zone/all$",             "type": mqtt_pb2.AllZonesData,     "desc": "Zones and beacons",          "qos": 0, "retained": False},
        {"pattern": r"^dt/edge/.+/message$",           "type": mqtt_pb2.BroadcastMessageData, "desc": "Broadcast message",      "qos": 0, "retained": False},
        {"pattern": r"^dt/edge/.+/panic-ack$",         "type": mqtt_pb2.PanicAckData,     "desc": "Panic acknowledgement",      "qos": 0, "retained": False},
        # ── Alert Config ────────────────────────────────────────────────────
        {"pattern": r"^dt/edge/alert/config$",         "type": mqtt_pb2.AllAlertConfigData.AlertConfigData, "desc": "Alert configuration",         "qos": 0, "retained": True},
        {"pattern": r"^dt/edge/alert/configQoS$",      "type": mqtt_pb2.AllAlertConfigData.AlertConfigData, "desc": "Alert configuration (QoS)",   "qos": 1, "retained": True},
        {"pattern": r"^dt/edge/alert/config/motionless$",       "type": mqtt_pb2.MotionlessConfigData,            "desc": "Motionless alert config",          "qos": 0, "retained": False},
        {"pattern": r"^dt/edge/alert/config/critically-low-hr$", "type": mqtt_pb2.CriticallyLowHrConfigData,        "desc": "Critically low HR alert config",    "qos": 0, "retained": False},
    ]

TOPIC_MAP: list[dict[str, Any]] = _build_topic_map()

# Field names whose bytes value is a MAC address
_MAC_FIELDS = frozenset({
    "beacon_mac", "device_mac", "start_mac", "end_mac",
    "beaconMac", "deviceMac", "startMac", "endMac", "mac", "bssid",
})
_HEX_FIELDS = frozenset({"bytes_value", "bytesValue"})


def _bytes_to_mac(raw: bytes) -> str:
    return ":".join(f"{b:02X}" for b in raw)


def _format_bytes_fields(d: dict) -> dict:
    result = {}
    for key, value in d.items():
        if isinstance(value, dict):
            result[key] = _format_bytes_fields(value)
        elif isinstance(value, list):
            result[key] = [_format_bytes_fields(i) if isinstance(i, dict) else i for i in value]
        elif isinstance(value, str) and key in _MAC_FIELDS:
            try:
                result[key] = _bytes_to_mac(base64.b64decode(value))
            except Exception:
                result[key] = value
        elif isinstance(value, str) and key in _HEX_FIELDS:
            try:
                result[key] = base64.b64decode(value).hex()
            except Exception:
                result[key] = value
        else:
            result[key] = value
    return result


def _find_topic_entry(topic: str):
    for entry in TOPIC_MAP:
        if re.match(entry["pattern"], topic):
            return entry
    return None


def decode_mqtt_message(topic: str, raw_bytes: bytes) -> dict:
    """Decode raw MQTT bytes into a structured result dict."""
    entry = _find_topic_entry(topic)

    result: dict = {
        "topic": topic,
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload_size": len(raw_bytes),
        "raw_b64": base64.b64encode(raw_bytes).decode(),
    }

    if not entry or not PROTO_AVAILABLE:
        result["decoded"] = None
        result["description"] = "UNMAPPED_TOPIC" if not entry else "Protobuf unavailable"
        result["message_type"] = "unknown"
        result["qos"] = 0
        result["retained"] = False
        result["error"] = None # Don't error out, just fallback gracefully
        return result

    msg_class = entry["type"]
    try:
        msg = msg_class()
        msg.ParseFromString(raw_bytes)
        decoded = MessageToDict(
            msg,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=False,
            use_integers_for_enums=False,
        )
        decoded = _format_bytes_fields(decoded)
        result["decoded"] = decoded
        result["description"] = entry["desc"]
        result["message_type"] = msg_class.DESCRIPTOR.name
        result["qos"] = entry["qos"]
        result["retained"] = entry["retained"]
        result["error"] = None
    except Exception as e:
        result["decoded"] = None
        result["description"] = entry["desc"]
        result["message_type"] = msg_class.DESCRIPTOR.name
        result["error"] = str(e)

    return result


def store_message(topic: str, msg: dict):
    """Save decoded message to the in-memory ring buffer."""
    with store_lock:
        if topic not in message_store:
            message_store[topic] = []
        message_store[topic].append(msg)
        if len(message_store[topic]) > MAX_MESSAGES_PER_TOPIC:
            message_store[topic] = message_store[topic][-MAX_MESSAGES_PER_TOPIC:]


# ===========================================================================
# MQTT Client
# ===========================================================================

def on_mqtt_connect(client, userdata, flags, rc, *args, **kwargs):
    global mqtt_status
    if rc == 0:
        mqtt_status["connected"] = True
        mqtt_status["error"] = ""
        print("[MQTT] Connected.")
        # Subscribe to all known pattern topics + wildcard
        client.subscribe("#", qos=0)
        socketio.emit("mqtt_status", mqtt_status)
    else:
        mqtt_status["connected"] = False
        mqtt_status["error"] = f"Connection refused (rc={rc})"
        socketio.emit("mqtt_status", mqtt_status)


def on_mqtt_disconnect(client, userdata, rc, *args, **kwargs):
    global mqtt_status
    mqtt_status["connected"] = False
    mqtt_status["error"] = "Disconnected" if rc != 0 else ""
    print(f"[MQTT] Disconnected (rc={rc})")
    socketio.emit("mqtt_status", mqtt_status)


def on_mqtt_message(client, userdata, message):
    topic = message.topic
    payload_bytes = message.payload

    # Try to decode as protobuf first; if payload is Base64-string, decode it
    raw = payload_bytes
    try:
        # Some brokers send payload already as raw bytes
        decoded_msg = decode_mqtt_message(topic, raw)
    except Exception:
        decoded_msg = {
            "topic": topic,
            "ts": datetime.now(timezone.utc).isoformat(),
            "payload_size": len(raw),
            "raw_b64": base64.b64encode(raw).decode(),
            "decoded": None,
            "message_type": "unknown",
            "description": "Parse error",
            "error": "Failed to process payload",
        }

    store_message(topic, decoded_msg)
    try:
        emit_queue.put_nowait(decoded_msg)
    except queue.Full:
        pass # Drop silently if queue is totally full to save memory


def start_mqtt(host: str, port: int, username: str = "", password_: str = "", use_tls: bool = False):
    global mqtt_handle, mqtt_status
    if not MQTT_AVAILABLE:
        mqtt_status["error"] = "paho-mqtt not installed"
        return

    if mqtt_handle:
        try:
            mqtt_handle.disconnect()
        except Exception:
            pass

    try:
        from paho.mqtt.enums import CallbackAPIVersion
        client = mqtt_client.Client(CallbackAPIVersion.VERSION2, protocol=mqtt_client.MQTTv311)
    except ImportError:
        client = mqtt_client.Client(protocol=mqtt_client.MQTTv311)
    if username:
        client.username_pw_set(username, password_)
    if use_tls:
        client.tls_set()

    client.on_connect = on_mqtt_connect
    client.on_disconnect = on_mqtt_disconnect
    client.on_message = on_mqtt_message

    mqtt_status = {"connected": False, "broker": f"{host}:{port}", "error": "Connecting…"}

    def _run():
        try:
            client.connect(host, port, keepalive=60)
            client.loop_forever()
        except Exception as e:
            mqtt_status["error"] = str(e)
            mqtt_status["connected"] = False
            socketio.emit("mqtt_status", mqtt_status)

    mqtt_handle = client
    t = threading.Thread(target=_run, daemon=True)
    t.start()


# ===========================================================================
# Flask Routes
# ===========================================================================

@app.route("/")
def index():
    if not is_authenticated():
        return render_template("login.html")
    return render_template("dashboard.html")


@app.route("/login", methods=["POST"])
def login():
    pw = request.form.get("password", "")
    if _hash_pw(pw) == HASHED_PASSWORD:
        session["authenticated"] = True
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Invalid password"}), 401


@app.route("/logout")
def logout():
    session.clear()
    return render_template("login.html")


@app.route("/api/mqtt/disconnect", methods=["POST"])
def api_mqtt_disconnect():
    global mqtt_handle, mqtt_status
    if not is_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    if mqtt_handle:
        try:
            mqtt_handle.disconnect()
        except Exception:
            pass
        mqtt_handle = None
    mqtt_status = {"connected": False, "broker": "", "error": ""}
    socketio.emit("mqtt_status", mqtt_status)
    return jsonify({"ok": True})


@app.route("/api/topics")
def api_topics():
    if not is_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    with store_lock:
        topics = {
            t: {"count": len(msgs), "last_ts": msgs[-1]["ts"] if msgs else None}
            for t, msgs in message_store.items()
        }
    return jsonify(topics)


@app.route("/api/messages/<path:topic>")
def api_messages(topic):
    if not is_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    with store_lock:
        msgs = message_store.get(topic, [])
    return jsonify(list(reversed(msgs)))  # newest first


@app.route("/api/decode", methods=["POST"])
def api_decode():
    """Manual one-off decode endpoint."""
    if not is_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json(force=True)
    topic = data.get("topic", "")
    payload_b64 = data.get("payload", "")
    try:
        raw = base64.b64decode(payload_b64)
        result = decode_mqtt_message(topic, raw)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/mqtt/connect", methods=["POST"])
def api_mqtt_connect():
    if not is_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json(force=True)
    host = data.get("host", "localhost")
    port = int(data.get("port", 1883))
    username = data.get("username", "")
    password_ = data.get("password", "")
    use_tls = data.get("tls", False)
    threading.Thread(
        target=start_mqtt, args=(host, port, username, password_, use_tls), daemon=True
    ).start()
    return jsonify({"ok": True, "broker": f"{host}:{port}"})


@app.route("/api/mqtt/status")
def api_mqtt_status():
    if not is_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify(mqtt_status)


@app.route("/api/supported-topics")
def api_supported_topics():
    if not is_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify([
        {
            "pattern": e["pattern"].lstrip("^").rstrip("$").replace(r".+", "<id>"),
            "message_type": e["type"].DESCRIPTOR.name if PROTO_AVAILABLE else "N/A",
            "description": e["desc"],
            "qos": e["qos"],
            "retained": e["retained"],
        }
        for e in TOPIC_MAP
    ])


@app.route("/api/proto/info")
def api_proto_info():
    """Return info about the currently loaded proto file."""
    if not is_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    proto_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "protos")
    proto_file = os.path.join(proto_dir, "mqtt.proto")
    if os.path.exists(proto_file):
        stat = os.stat(proto_file)
        return jsonify({
            "filename": "mqtt.proto",
            "size_bytes": stat.st_size,
            "modified_ts": stat.st_mtime,
            "topic_count": len(TOPIC_MAP),
        })
    return jsonify({"filename": None, "topic_count": len(TOPIC_MAP)})


@app.route("/api/proto/upload", methods=["POST"])
def api_proto_upload():
    """
    Upload a new .proto file, compile it, hot-reload mqtt_pb2, and rebuild TOPIC_MAP.
    Expects multipart/form-data with field 'proto_file'.
    """
    global mqtt_pb2, PROTO_AVAILABLE, TOPIC_MAP

    if not is_authenticated():
        return jsonify({"error": "Unauthorized"}), 401

    if "proto_file" not in request.files:
        return jsonify({"error": "No file part in request"}), 400

    f = request.files["proto_file"]
    if not f.filename:
        return jsonify({"error": "No file selected"}), 400
    if not f.filename.lower().endswith(".proto"):
        return jsonify({"error": "Only .proto files are accepted"}), 400

    proto_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "protos")
    proto_path = os.path.join(proto_dir, "mqtt.proto")
    root_dir = os.path.dirname(os.path.abspath(__file__))

    # Save uploaded file (always overwrites mqtt.proto)
    try:
        f.save(proto_path)
    except Exception as e:
        return jsonify({"error": f"Failed to save file: {e}"}), 500

    # Compile the new proto
    try:
        subprocess.check_output(
            [
                sys.executable, "-m", "grpc_tools.protoc",
                f"-I{proto_dir}",
                f"--python_out={root_dir}",
                proto_path,
            ],
            stderr=subprocess.STDOUT,
        )
    except subprocess.CalledProcessError as e:
        return jsonify({"error": f"Protobuf compilation failed:\n{e.output.decode()}"}), 422

    # Soft-restart the server to reload mqtt_pb2 and rebuild the pool
    try:
        threading.Thread(target=restart_server, daemon=True).start()
    except Exception as e:
        print(f"[RESTART ERROR] {e}")
    
    PROTO_AVAILABLE = True # Assume success after restart

    # Rebuild TOPIC_MAP with the new mqtt_pb2
    try:
        TOPIC_MAP = _build_topic_map()
    except Exception as e:
        return jsonify({"error": f"Failed to rebuild topic map: {e}"}), 500

    print(f"[PROTO] Hot-reloaded from uploaded file. {len(TOPIC_MAP)} topic patterns active.")
    socketio.emit("proto_reloaded", {"topic_count": len(TOPIC_MAP), "filename": f.filename})

    return jsonify({
        "ok": True,
        "filename": f.filename,
        "topic_count": len(TOPIC_MAP),
        "message": "Proto uploaded. Server is restarting to apply changes... Page will refresh automatically.",
    })
# ===========================================================================
# SocketIO events
# ===========================================================================

@socketio.on("connect")
def on_ws_connect():
    if not is_authenticated():
        return False  # reject unauthenticated WS connections
    emit("mqtt_status", mqtt_status)


@socketio.on("manual_decode")
def on_manual_decode(data):
    if not is_authenticated():
        return
    topic = data.get("topic", "")
    payload_b64 = data.get("payload", "")
    try:
        raw = base64.b64decode(payload_b64)
        result = decode_mqtt_message(topic, raw)
        emit("decode_result", result)
    except Exception as e:
        emit("decode_result", {"error": str(e)})


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("  MQTT Protobuf Decoder — Internal Dashboard")
    print("=" * 60)
    print(f"  URL     : http://localhost:8080")
    print(f"  Password: {ACCESS_PASSWORD}")
    print("=" * 60)
    socketio.run(app, host="0.0.0.0", port=8080, debug=False)
