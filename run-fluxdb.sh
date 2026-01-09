#!/bin/bash
# FluxDB - Start All Services (Linux/macOS)
# This script starts both the FluxDB backend server and the FluxDB Studio frontend

echo ""
echo "========================================"
echo "  FluxDB - Starting All Services"
echo "========================================"
echo ""

# Check if Rust is installed for backend
if ! command -v cargo &> /dev/null; then
    echo "[ERROR] Cargo not found! Please install Rust from https://rustup.rs"
    exit 1
fi

# Check if Node.js is installed for frontend
if ! command -v node &> /dev/null; then
    echo "[ERROR] Node.js not found! Please install Node.js from https://nodejs.org"
    exit 1
fi

# Get the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

echo "[1/4] Building FluxDB server..."
cd fluxdb
cargo build --release
if [ $? -ne 0 ]; then
    echo "[ERROR] Failed to build FluxDB server"
    exit 1
fi

echo "[2/4] Starting FluxDB server in background..."
cargo run --release --bin fluxdb-server &
SERVER_PID=$!

# Wait for server to start
echo "Waiting for FluxDB server to start..."
sleep 3

echo "[3/4] Installing frontend dependencies..."
cd ../fluxdb-studio
npm install
if [ $? -ne 0 ]; then
    echo "[ERROR] Failed to install npm dependencies"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

echo "[4/4] Starting FluxDB Studio..."
echo ""
echo "========================================"
echo "  FluxDB server running on :8086"
echo "  FluxDB Studio starting..."
echo "========================================"
echo ""

# Start in dev mode
npm run dev

# Cleanup on exit
trap "kill $SERVER_PID 2>/dev/null" EXIT
