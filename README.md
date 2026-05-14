# MQTT Protobuf Decoder Ultimate

A high-performance, real-time MQTT dashboard designed to decode binary Protobuf payloads into human-readable JSON. Hosted on AWS with automatic CI/CD integration.

## 🚀 Live Environment
*   **Production URL**: [http://54.162.252.58:8080](http://54.162.252.58:8080)
*   **Production Password**: `decoder@solx`

## 🏗 Infrastructure Details
*   **Host**: AWS EC2 (t3.micro)
*   **OS**: Ubuntu 22.04 LTS
*   **Process Manager**: `systemd` (Service: `mqtt-dashboard`)
*   **WSGI Server**: Gunicorn (using `eventlet` worker)

## 🔄 CI/CD Pipeline
This repository uses **GitHub Actions** for automatic deployment. 
*   **Workflow**: `.github/workflows/deploy.yml`
*   **Trigger**: Any push or merge to the `main` branch.
*   **Actions**:
    1. Connects to AWS via SSH.
    2. Pulls latest code from GitHub.
    3. Restarts the `mqtt-dashboard` system service.

## 🛠 Setup & Development

### Local Installation
```bash
# 1. Setup virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run development server
python3 app.py
```
*   **Local URL**: `http://localhost:8080`
*   **Local Password**: `decoder2025`

### Server Management (AWS)
```bash
# Check service status
sudo systemctl status mqtt-dashboard

# Restart service manually
sudo systemctl restart mqtt-dashboard

# View real-time logs
sudo journalctl -u mqtt-dashboard -f
```

## 🔒 Security
The `ACCESS_PASSWORD` is managed via environment variables. 
*   **Production**: Set in `/etc/systemd/system/mqtt-dashboard.service`.
*   **Local**: Defaults to `decoder2025` in `app.py`.

## 📂 Key Files
*   `app.py`: Main Flask & SocketIO application logic.
*   `templates/dashboard.html`: Futuristic UI with background-sync persistence.
*   `mqtt-dashboard.service`: Systemd configuration for AWS deployment.
*   `.github/workflows/deploy.yml`: GitHub Actions deployment script.
