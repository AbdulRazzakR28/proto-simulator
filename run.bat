@echo off
:: ==============================================================================
:: MQTT Decoder Dashboard - One-Click Launcher (Windows)
:: ==============================================================================

echo ==============================================
echo     MQTT Protobuf Decoder - Setup ^& Run
echo ==============================================

:: Check for Python
where python >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] python is not installed or not in PATH.
    echo Please install Python from https://www.python.org/
    pause
    exit /b 1
)

:: Create virtual environment if it doesn't exist
if not exist .venv (
    echo [INFO] Creating virtual environment...
    python -m venv .venv
)

:: Activate virtual environment
call .venv\Scripts\activate.bat

:: Install/Update requirements
echo [INFO] Installing dependencies...
python -m pip install -q --upgrade pip
pip install -q -r requirements.txt

:: Final check for grpcio-tools (needed for proto compilation)
pip show grpcio-tools >nul 2>nul
if %errorlevel% neq 0 (
    echo [INFO] Installing grpcio-tools for proto support...
    pip install -q grpcio-tools
)

echo [SUCCESS] Setup complete!
echo Starting dashboard on http://localhost:8080...
echo (Close this window to stop)

:: Run the app
python app.py
pause
