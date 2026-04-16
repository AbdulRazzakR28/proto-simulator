# 🚀 Simple Operations Installation Guide
## MQTT Protobuf Decoder Dashboard

> [!TIP]
> **Quick Download**: You can download the entire project as a ZIP file here: [**Download Project ZIP**](/download/zip)

---

This guide is designed for the Operations team. It will help you get the dashboard running on your computer even if you have zero technical or programming experience.

---

## 🛠 Step 0: Install Python (Must be done first)

The only thing you need on your computer is **Python**. If you don't have it, follow these steps:

### For Windows:
1.  Go to [python.org/downloads](https://www.python.org/downloads/) and click the big yellow **"Download Python"** button.
2.  Open the downloaded installer.
3.  > [!IMPORTANT]
    > **Crucial Step**: In the installer, check the box that says **"Add Python to PATH"** before you click "Install Now".
4.  Wait for the installation to finish.

### For Mac:
1.  Open your terminal (Press `Cmd + Space` and type "Terminal").
2.  Type `python3 --version` and press Enter. If it shows a version number, you are ready!
3.  If not, download and install it from [python.org/downloads](https://www.python.org/downloads/).

---

## 🏗 Choose your Way to Start

### Option 1: The "Double-Click" Method (Recommended)
This is the easiest way. It does all the setup for you.

1.  Open the project folder on your computer.
2.  **On Windows**: Double-click the file named `run.bat`.
3.  **On Mac**: Right-click the file named `run`. Choose "Open With" -> "Terminal".
4.  A window will pop up and start installing things. Do not close it.
5.  Once you see the message **"Starting dashboard on http://localhost:8080"**, you are done! Open that link in your browser.

---

### Option 2: The "Just Run This" Method
If the scripts above don't work, you can try this instead.

1.  Open a Terminal or Command Prompt window.
2.  Navigate to the project folder (or just drag the folder into the terminal window).
3.  Type the following and press Enter:
    `python start_dashboard.py`
4.  This script will automatically fix any missing files and start the site for you.

---

### Option 3: Manual Step-by-Step (The "Trust No One" Method)
If you prefer to type the commands yourself, here are the 3 exact steps:

1.  **Open your terminal** in the project folder.
2.  **Install the requirements** (Copy and paste this):
    `pip install -r requirements.txt`
3.  **Start the server** (Copy and paste this):
    `python app.py`
4.  Wait for the "Starting dashboard..." message and open [http://localhost:8080](http://localhost:8080).

---

## 📱 How to view it on your phone
1. Ensure your phone and your computer are on the **same Wi-Fi**.
2. Find your computer's IP address (e.g., `192.168.1.10`).
3. On your phone browser, go to `http://192.168.1.10:8080`.

## 🆘 Troubleshooting
- **"Command not found"**: This usually means Python wasn't added to your "PATH" during installation (see Step 0).
- **"Access Denied"**: Try running your terminal as an Administrator (Right-click "Command Prompt" -> "Run as Administrator").
