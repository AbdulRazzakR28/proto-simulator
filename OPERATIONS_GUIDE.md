# 🚀 MQTT Protobuf Decoder Operations Guide

This guide is designed for the Operations and Development teams to manage the MQTT Decoder dashboard.

---

## ☁️ Option 1: Live Cloud Dashboard (Easiest)
The dashboard is hosted on AWS and is always running. You don't need to install anything.

*   **URL**: [http://54.162.252.58:8080](http://54.162.252.58:8080)
*   **Password**: `decoder@solx`
*   **Status**: Always Online (24/7)

---

## 💻 Option 2: Local Desktop Version (For Development)
If you need to run the dashboard on your own Mac or PC:

1.  **Install Python**: Download from [python.org](https://www.python.org/downloads/).
2.  **Start Dashboard**:
    *   **Mac**: Right-click `run.sh` -> Open With -> Terminal.
    *   **Windows**: Double-click `run.bat`.
3.  **Access**: Open [http://localhost:8080](http://localhost:8080) in your browser.
4.  **Local Password**: `decoder2025`

---

## 🔄 Automatic Sync (For Developers)
The dashboard is equipped with an **Automatic Pipeline**. When you make code changes on your local machine:

1.  Push your changes to the `main` branch on GitHub.
2.  GitHub Actions will automatically connect to AWS and update the site.
3.  The AWS site will refresh within 30-60 seconds.

---

## 🛠 Features & Improvements
*   **Background Sync**: The dashboard now remains active in background tabs. If you switch away and come back, it will instantly refresh with new messages.
*   **Environment Selection**: On connection, you must manually select the target environment (SIT, Prod, or Local) from the dropdown.
*   **Hot-Reload Proto**: You can upload a new `.proto` file directly through the UI without restarting the server.

---

## 🆘 Troubleshooting
*   **Site not loading?** Check if the AWS instance is running or if Port 8080 is blocked by your corporate VPN.
*   **Login Failed?** Double check if you are using `decoder@solx` (Cloud) or `decoder2025` (Local).
