#!/bin/bash
# ==============================================================================
# MQTT Decoder Dashboard - One-Click Launcher (Mac/Linux)
# ==============================================================================

# Define colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}==============================================${NC}"
echo -e "${GREEN}    MQTT Protobuf Decoder - Setup & Run${NC}"
echo -e "${BLUE}==============================================${NC}"

# Check for Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: python3 is not installed.${NC}"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python3 -m venv .venv
fi

# Activate virtual environment
source .venv/bin/activate

# Install/Update requirements
echo -e "${YELLOW}Installing dependencies...${NC}"
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Final check for grpcio-tools (needed for proto compilation)
if ! pip show grpcio-tools &> /dev/null; then
    echo -e "${YELLOW}Installing grpcio-tools for proto support...${NC}"
    pip install -q grpcio-tools
fi

echo -e "${GREEN}✓ Setup complete!${NC}"
echo -e "${BLUE}Starting dashboard on http://localhost:8080...${NC}"
echo -e "${YELLOW}(Press Ctrl+C to stop)${NC}"

# Run the app
python3 app.py
