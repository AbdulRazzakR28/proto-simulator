#!/usr/bin/env python3
import os
import sys
import subprocess
import importlib.util

"""
MQTT Decoder Dashboard - Smart Launcher
Automatically checks and installs dependencies before starting the app.
"""

REQUIRED_PACKAGES = [
    "flask",
    "flask-socketio",
    "paho-mqtt",
    "protobuf",
    "grpcio-tools"
]

def check_package(package):
    """Check if a package is installed."""
    # Special mapping for packages where import name != pip name
    import_map = {
        "flask-socketio": "flask_socketio",
        "grpcio-tools": "grpc_tools"
    }
    import_name = import_map.get(package, package)
    return importlib.util.find_spec(import_name) is not None

def install_package(package):
    """Install a package via pip."""
    print(f"[INFO] Installing missing package: {package}...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return True
    except Exception as e:
        print(f"[ERROR] Failed to install {package}: {e}")
        return False

def main():
    print("=" * 60)
    print("      MQTT Protobuf Decoder - Smart Launcher")
    print("=" * 60)

    # 1. Verify Dependencies
    missing = [pkg for pkg in REQUIRED_PACKAGES if not check_package(pkg)]
    
    if missing:
        print(f"[INFO] Missing {len(missing)} dependencies. Setting up environment...")
        for pkg in missing:
            if not install_package(pkg):
                print(f"[CRITICAL] Could not fulfill dependencies. Exiting.")
                sys.exit(1)
        print("[SUCCESS] All dependencies installed.")
    else:
        print("[INFO] All dependencies already satisfied.")

    # 2. Start the App
    print("[INFO] Launching dashboard...")
    try:
        # We use subprocess.call to let the app take over the terminal
        subprocess.call([sys.executable, "app.py"])
    except KeyboardInterrupt:
        print("\n[INFO] Dashboard stopped by user.")
    except Exception as e:
        print(f"[ERROR] Launch failed: {e}")

if __name__ == "__main__":
    main()
