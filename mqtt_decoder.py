6#!/usr/bin/env python3
"""
Universal MQTT Protobuf Message Decoder

Decodes any MQTT message payload from topics defined in mqtt.proto.

Usage:
  python3 mqtt_decoder.py <topic> <base64_payload>
  python3 mqtt_decoder.py --list                     # List all supported topics
  python3 mqtt_decoder.py --interactive              # Interactive mode

Examples:
  python3 mqtt_decoder.py "dt/edge/alert/config" "Ck8KKA0A..."
  python3 mqtt_decoder.py "dt/watch01/sensor/heartrate" "CICA..."
  python3 mqtt_decoder.py "cmd/edge/auth/req" "CICA..."
"""

import sys
import os
import re
import base64
import json
import argparse
from google.protobuf.json_format import MessageToDict, MessageToJson
from google.protobuf.descriptor import FieldDescriptor

try:
    import paho.mqtt.client as mqtt_client
    HAS_MQTT = True
except ImportError:
    HAS_MQTT = False

# Add the directory containing mqtt_pb2 to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# Auto-compile proto before import if exists
import subprocess
try:
    proto_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "protos", "mqtt.proto")
    if os.path.exists(proto_path):
        subprocess.check_call([
            sys.executable, "-m", "grpc_tools.protoc",
            f"-I{os.path.dirname(proto_path)}",
            f"--python_out={os.path.dirname(os.path.abspath(__file__))}",
            proto_path
        ])
except Exception:
    pass

import mqtt_pb2


# ═══════════════════════════════════════════════════════════════════════════════
# Topic → Message Type Mapping
# ═══════════════════════════════════════════════════════════════════════════════
# Topics use patterns where <watch-id>, <device-id>, <user-id> are variable.
# We use regex patterns to match incoming topics to message types.

TOPIC_MAP = [
    # ── Command Topics ──────────────────────────────────────────────────────
    {
        "pattern": r"^cmd/edge/auth/req$",
        "message_type": mqtt_pb2.Command,
        "description": "Sign in/out command request",
        "qos": 1, "retained": False,
    },
    {
        "pattern": r"^cmd/.+/auth/res$",
        "message_type": mqtt_pb2.CommandResponse,
        "description": "Sign in/out command response",
        "qos": 1, "retained": False,
    },
    {
        "pattern": r"^cmd/edge/user/eula$",
        "message_type": mqtt_pb2.Command,
        "description": "Set EULA version command",
        "qos": 1, "retained": False,
    },
    {
        "pattern": r"^cmd/.+/user/eula$",
        "message_type": mqtt_pb2.CommandResponse,
        "description": "Set EULA version response",
        "qos": 1, "retained": False,
    },
    {
        "pattern": r"^cmd/edge/panic/req$",
        "message_type": mqtt_pb2.Command,
        "description": "Trigger/dismiss panic command",
        "qos": 1, "retained": False,
    },
    {
        "pattern": r"^cmd/.+/panic/res$",
        "message_type": mqtt_pb2.CommandResponse,
        "description": "Panic command response",
        "qos": 1, "retained": False,
    },
    {
        "pattern": r"^cmd/edge/user/req$",
        "message_type": mqtt_pb2.Command,
        "description": "Set personal data (DOB) command",
        "qos": 1, "retained": False,
    },
    {
        "pattern": r"^cmd/.+/user/res$",
        "message_type": mqtt_pb2.CommandResponse,
        "description": "Personal data command response",
        "qos": 1, "retained": False,
    },
    {
        "pattern": r"^cmd/edge/wrh/req$",
        "message_type": mqtt_pb2.Command,
        "description": "Log work hours command",
        "qos": 1, "retained": False,
    },
    {
        "pattern": r"^cmd/.+/wrh/res$",
        "message_type": mqtt_pb2.CommandResponse,
        "description": "Work hours command response",
        "qos": 1, "retained": False,
    },

    # ── Sensor Data Topics ──────────────────────────────────────────────────
    {
        "pattern": r"^dt/.+/sensor/beacon$",
        "message_type": mqtt_pb2.WatchSensorData,
        "description": "Beacon sensor data",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/.+/sensor/heartrate$",
        "message_type": mqtt_pb2.WatchSensorData,
        "description": "Heart rate sensor data",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/.+/sensor/hrv$",
        "message_type": mqtt_pb2.WatchSensorData,
        "description": "Heart rate variability data",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/.+/sensor/humidity$",
        "message_type": mqtt_pb2.WatchSensorData,
        "description": "Humidity sensor data",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/.+/sensor/noise$",
        "message_type": mqtt_pb2.WatchSensorData,
        "description": "Noise sensor data",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/.+/sensor/steps$",
        "message_type": mqtt_pb2.WatchSensorData,
        "description": "Steps sensor data",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/.+/sensor/temp$",
        "message_type": mqtt_pb2.WatchSensorData,
        "description": "Temperature sensor data",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/.+/sensor/heatIndex$",
        "message_type": mqtt_pb2.WatchHeatIndexData,
        "description": "Heat index sensor data",
        "qos": 0, "retained": False,
    },

    # ── Event Topics ────────────────────────────────────────────────────────
    {
        "pattern": r"^dt/.+/evt$",
        "message_type": mqtt_pb2.WatchEventData,
        "description": "Watch event data (fall, startup, crew assist, geofence, etc.)",
        "qos": 0, "retained": False,
    },

    # ── Metric Topics ───────────────────────────────────────────────────────
    {
        "pattern": r"^dt/.+/metric/battery$",
        "message_type": mqtt_pb2.WatchMetricData,
        "description": "Battery metric data",
        "qos": 1, "retained": False,
    },
    {
        "pattern": r"^dt/.+/metric/screen$",
        "message_type": mqtt_pb2.WatchMetricData,
        "description": "Screen metric data",
        "qos": 1, "retained": False,
    },
    {
        "pattern": r"^dt/.+/metric/wifi$",
        "message_type": mqtt_pb2.WatchMetricData,
        "description": "WiFi metric data",
        "qos": 1, "retained": False,
    },

    # ── Edge Data Topics ────────────────────────────────────────────────────
    {
        "pattern": r"^dt/edge/app/watch/version$",
        "message_type": mqtt_pb2.AppVersionData,
        "description": "Current watch app version",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/edge/geofence/active$",
        "message_type": mqtt_pb2.AllGeofencesData,
        "description": "Current active geofences",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/edge/time$",
        "message_type": mqtt_pb2.EdgeTimeData,
        "description": "Current server time",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/edge/vessel/offset$",
        "message_type": mqtt_pb2.VesselOffsetData,
        "description": "Current vessel UTC offset",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/edge/.+/user$",
        "message_type": mqtt_pb2.WatchUserData,
        "description": "Current logged-in user on watch",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/edge/user.+/wrh$",
        "message_type": mqtt_pb2.WrhSummaryData,
        "description": "Work rest hour summary for user",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/edge/zone/all$",
        "message_type": mqtt_pb2.AllZonesData,
        "description": "Current zones and beacons",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/edge/.+/message$",
        "message_type": mqtt_pb2.BroadcastMessageData,
        "description": "Broadcast message",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/edge/.+/panic-ack$",
        "message_type": mqtt_pb2.PanicAckData,
        "description": "Acknowledge panic message",
        "qos": 0, "retained": False,
    },

    # ── Alert Config Topics ─────────────────────────────────────────────────
    {
        "pattern": r"^dt/edge/alert/config$",
        "message_type": mqtt_pb2.AllAlertConfigData.AlertConfigData,
        "description": "Alert configuration (heat, cold, noise, fall)",
        "qos": 0, "retained": True,
    },
    {
        "pattern": r"^dt/edge/alert/configQoS$",
        "message_type": mqtt_pb2.AllAlertConfigData.AlertConfigData,
        "description": "Alert configuration (heat, cold, noise, fall) — QoS variant",
        "qos": 1, "retained": True,
    },
    {
        "pattern": r"^dt/edge/alert/config/motionless$",
        "message_type": mqtt_pb2.MotionlessConfigData,
        "description": "Motionless alert configuration",
        "qos": 0, "retained": False,
    },
    {
        "pattern": r"^dt/edge/alert/config/critically-low-hr$",
        "message_type": mqtt_pb2.CriticallyLowHrConfigData,
        "description": "Critically low HR alert configuration",
        "qos": 0, "retained": False,
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

# Maps for enum display names
EVENT_TYPE_NAMES = {
    0: "GENERIC", 1: "FALL", 2: "STARTUP", 3: "CREW_ASSIST_ACK",
    4: "GEOFENCE_ALERT_ACK", 5: "GEOFENCE_UPDATE", 6: "HEAT_ALERT_LOG",
    7: "NOISE_ALERT_LOG", 8: "PROLONGED_HEAT", 9: "GEOFENCE_BREACH",
    10: "WRH_NON_CONFORMANCE", 11: "HIGH_HEAT", 12: "HIGH_NOISE",
    13: "PROLONGED_COLD",
}


def bytes_to_mac(b: bytes) -> str:
    """Convert raw bytes to MAC address string."""
    return ":".join(f"{x:02X}" for x in b)


_MAC_FIELDS = frozenset({
    "beacon_mac", "device_mac", "start_mac", "end_mac",
    "beaconMac", "deviceMac", "startMac", "endMac", "mac", "bssid",
})
_HEX_FIELDS = frozenset({"bytes_value", "bytesValue"})

def format_bytes_fields(d: dict, msg=None) -> dict:
    """
    Walk through decoded dict and format bytes fields nicely.
    - beacon_mac / mac / bssid / device_mac → MAC address
    - other bytes → hex string
    """
    result = {}
    for key, value in d.items():
        if isinstance(value, dict):
            # Recurse into sub-messages
            result[key] = format_bytes_fields(value, None)
        elif isinstance(value, list):
            result[key] = [
                format_bytes_fields(item, None) if isinstance(item, dict) else item
                for item in value
            ]
        elif isinstance(value, str) and key in _MAC_FIELDS:
            # These are base64-encoded bytes in the JSON dict; decode to MAC
            try:
                raw = base64.b64decode(value)
                result[key] = bytes_to_mac(raw)
            except Exception:
                result[key] = value
        elif isinstance(value, str) and key in _HEX_FIELDS:
            try:
                raw = base64.b64decode(value)
                result[key] = raw.hex()
            except Exception:
                result[key] = value
        else:
            result[key] = value
    return result


def find_topic_entry(topic: str):
    """Find the matching topic entry for a given MQTT topic string."""
    for entry in TOPIC_MAP:
        if re.match(entry["pattern"], topic):
            return entry
    return None


def decode_payload(topic: str, payload_b64: str) -> dict:
    """
    Decode a base64-encoded protobuf payload for the given MQTT topic.

    Returns a dict with:
      - topic: the MQTT topic
      - description: human-readable description
      - message_type: the protobuf message class name  
      - decoded: the decoded message as a dict
      - qos / retained: MQTT properties
    """
    entry = find_topic_entry(topic)
    if entry is None:
        return {
            "topic": topic,
            "description": "UNMAPPED_TOPIC",
            "message_type": "unknown",
            "qos": 0,
            "retained": False,
            "payload_size_bytes": len(base64.b64decode(payload_b64)),
            "decoded": None,
        }

    # Decode base64 → raw bytes
    try:
        raw = base64.b64decode(payload_b64)
    except Exception as e:
        raise ValueError(f"Invalid base64 payload: {e}")

    # Parse into protobuf message
    msg_class = entry["message_type"]
    msg = msg_class()
    try:
        msg.ParseFromString(raw)
    except Exception as e:
        raise ValueError(f"Failed to parse as {msg_class.DESCRIPTOR.name}: {e}")

    # Convert to dict with enum names preserved
    decoded = MessageToDict(
        msg,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=False,
        use_integers_for_enums=False,
    )

    # Format bytes fields
    decoded = format_bytes_fields(decoded, msg)

    return {
        "topic": topic,
        "description": entry["description"],
        "message_type": msg_class.DESCRIPTOR.name,
        "qos": entry["qos"],
        "retained": entry["retained"],
        "payload_size_bytes": len(raw),
        "decoded": decoded,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Pretty Printer
# ═══════════════════════════════════════════════════════════════════════════════

BOLD = "\033[1m"
DIM = "\033[2m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
MAGENTA = "\033[35m"
RESET = "\033[0m"


def pretty_print_result(result: dict):
    """Print the decoded result in a human-readable format."""
    print()
    print(f"{BOLD}{'═' * 64}{RESET}")
    print(f"{BOLD}{CYAN}  MQTT Protobuf Decoder{RESET}")
    print(f"{BOLD}{'═' * 64}{RESET}")
    print()
    print(f"  {DIM}Topic:{RESET}        {BOLD}{result['topic']}{RESET}")
    print(f"  {DIM}Description:{RESET}  {result['description']}")
    print(f"  {DIM}Message Type:{RESET} {GREEN}{result['message_type']}{RESET}")
    print(f"  {DIM}QoS:{RESET}          {result['qos']}")
    print(f"  {DIM}Retained:{RESET}     {result['retained']}")
    print(f"  {DIM}Payload Size:{RESET} {result['payload_size_bytes']} bytes")
    print()
    print(f"{BOLD}{'─' * 64}{RESET}")
    print(f"{BOLD}{YELLOW}  Decoded Payload:{RESET}")
    print(f"{BOLD}{'─' * 64}{RESET}")
    print()
    print(json.dumps(result["decoded"], indent=2, ensure_ascii=False))
    print()
    print(f"{BOLD}{'═' * 64}{RESET}")
    print()


def print_topic_list():
    """Print all supported topics."""
    print()
    print(f"{BOLD}{'═' * 74}{RESET}")
    print(f"{BOLD}{CYAN}  Supported MQTT Topics{RESET}")
    print(f"{BOLD}{'═' * 74}{RESET}")
    print()
    print(f"  {BOLD}{'Topic Pattern':<40} {'Message Type':<22} QoS  Ret{RESET}")
    print(f"  {'─' * 70}")

    for entry in TOPIC_MAP:
        pattern = entry["pattern"].replace(r"^", "").replace(r"$", "").replace(r".+", "<id>")
        msg_name = entry["message_type"].DESCRIPTOR.name
        qos = entry["qos"]
        retained = "Yes" if entry["retained"] else "No"
        print(f"  {pattern:<40} {msg_name:<22} {qos}    {retained}")

    print()
    print(f"  {DIM}Total: {len(TOPIC_MAP)} topic patterns{RESET}")
    print(f"{BOLD}{'═' * 74}{RESET}")
    print()


def interactive_mode():
    """Run in interactive mode — repeatedly ask for topic + payload."""
    print()
    print(f"{BOLD}{CYAN}═══ MQTT Protobuf Decoder — Interactive Mode ═══{RESET}")
    print(f"{DIM}Type 'quit' or 'exit' to stop. Type 'list' to see topics.{RESET}")
    print()

    while True:
        try:
            topic = input(f"{BOLD}Topic:{RESET} ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not topic:
            continue
        if topic.lower() in ("quit", "exit", "q"):
            print("Bye!")
            break
        if topic.lower() == "list":
            print_topic_list()
            continue

        try:
            payload = input(f"{BOLD}Payload (base64):{RESET} ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not payload:
            print(f"{YELLOW}Empty payload, skipping.{RESET}\n")
            continue

        try:
            result = decode_payload(topic, payload)
            pretty_print_result(result)
        except ValueError as e:
            print(f"\n{BOLD}\033[31mError:{RESET} {e}\n")


def live_mode(host, port, username, password, sub_topic, use_ssl=False, mqtt_version=3):
    """Run in live mode - subscribe to an MQTT broker and decode on the fly."""
    if not HAS_MQTT:
        print(f"\n{BOLD}\033[31mError:{RESET} paho-mqtt is not installed. Please run `pip install paho-mqtt` to use live mode.\n")
        sys.exit(1)

    print()
    print(f"{BOLD}{CYAN}═══ MQTT Protobuf Decoder — Live Mode ═══{RESET}")
    ssl_str = f"| SSL: {GREEN}ON{RESET} " if use_ssl else ""
    print(f"{DIM}Connecting to {host}:{port} {ssl_str}| Subscribing to '{sub_topic}'{RESET}")
    print(f"{DIM}Waiting for messages... (Press Ctrl+C to quit){RESET}")
    print()

    def on_connect(client, userdata, flags, rc, *args, **kwargs):
        if rc == 0:
            print(f"{GREEN}✓ Connected successfully.{RESET}")
            client.subscribe(sub_topic)
        else:
            print(f"{BOLD}\033[31m✗ Connection failed with code {rc}{RESET}")

    def on_message(client, userdata, msg):
        # We attempt to decode by converting raw bytes to base64, 
        # so we can reuse the existing `decode_payload` function unmodified.
        payload_b64 = base64.b64encode(msg.payload).decode('utf-8')
        try:
            result = decode_payload(msg.topic, payload_b64)
            pretty_print_result(result)
        except ValueError as e:
            # Ignore messages that do not match our topic patterns or fail to parse
            pass

    protocol = mqtt_client.MQTTv5 if mqtt_version == 5 else mqtt_client.MQTTv311
    try:
        from paho.mqtt.enums import CallbackAPIVersion
        client = mqtt_client.Client(CallbackAPIVersion.VERSION2, protocol=protocol)
    except ImportError:
        client = mqtt_client.Client(protocol=protocol)

    if use_ssl:
        import ssl
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

    if username is not None:
        client.username_pw_set(username, password)
        
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(host, port, 60)
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nBye!")
    except Exception as e:
        print(f"\n{BOLD}\033[31mError connecting to broker:{RESET} {e}\n")


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Decode MQTT protobuf messages defined in mqtt.proto",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "dt/edge/alert/config" "Ck8KKA0AABxC..."
  %(prog)s "dt/watch01/sensor/heartrate" "CICA..."
  %(prog)s "cmd/edge/auth/req" "CICA..."
  %(prog)s --list
  %(prog)s --interactive
        """,
    )
    parser.add_argument("topic", nargs="?", help="MQTT topic string")
    parser.add_argument("payload", nargs="?", help="Base64-encoded protobuf payload")
    parser.add_argument("--list", action="store_true", help="List all supported topics")
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive mode")
    parser.add_argument("--json", action="store_true", help="Output raw JSON (no colors)")
    
    # Live mode arguments
    live_group = parser.add_argument_group('Live Mode')
    live_group.add_argument("--live", action="store_true", help="Connect to MQTT broker and decode live messages")
    live_group.add_argument("--host", default="sitlng.edge.solx.sg-lab.safevue.ai", help="MQTT broker host (default: sitlng.edge.solx.sg-lab.safevue.ai)")
    live_group.add_argument("--port", type=int, default=1883, help="MQTT broker port (default: 1883)")
    live_group.add_argument("--username", help="MQTT broker username")
    live_group.add_argument("--password", help="MQTT broker password")
    live_group.add_argument("--sub-topic", default="#", help="MQTT topic to subscribe to in live mode (default: #)")
    live_group.add_argument("--ssl", action="store_true", help="Use SSL/TLS secure connection")
    live_group.add_argument("--mqtt-version", type=int, choices=[3, 5], default=3, help="MQTT Protocol Version (3 for v3.1.1, 5 for v5)")

    args = parser.parse_args()

    if args.list:
        print_topic_list()
        return

    if args.interactive:
        interactive_mode()
        return

    if args.live:
        live_mode(args.host, args.port, args.username, args.password, args.sub_topic, args.ssl, args.mqtt_version)
        return

    if not args.topic or not args.payload:
        parser.print_help()
        sys.exit(1)

    try:
        result = decode_payload(args.topic, args.payload)
        if args.json:
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            pretty_print_result(result)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
