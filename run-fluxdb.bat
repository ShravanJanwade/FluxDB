@echo off
REM FluxDB - Start All Services (Windows)
REM This script starts both the FluxDB backend server and the FluxDB Studio frontend

echo.
echo ========================================
echo   FluxDB - Starting All Services
echo ========================================
echo.

REM Check if Rust is installed for backend
where cargo >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Cargo not found! Please install Rust from https://rustup.rs
    pause
    exit /b 1
)

REM Check if Node.js is installed for frontend
where node >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Node.js not found! Please install Node.js from https://nodejs.org
    pause
    exit /b 1
)

echo [1/4] Building FluxDB server...
cd fluxdb
cargo build --release
if %errorlevel% neq 0 (
    echo [ERROR] Failed to build FluxDB server
    pause
    exit /b 1
)

echo [2/4] Starting FluxDB server in background...
start "FluxDB Server" cargo run --release --bin fluxdb-server

REM Wait for server to start
echo Waiting for FluxDB server to start...
timeout /t 3 /nobreak >nul

echo [3/4] Installing frontend dependencies...
cd ..\fluxdb-studio
call npm install
if %errorlevel% neq 0 (
    echo [ERROR] Failed to install npm dependencies
    pause
    exit /b 1
)

echo [4/4] Starting FluxDB Studio...
echo.
echo ========================================
echo   FluxDB server running on :8086
echo   FluxDB Studio starting...
echo ========================================
echo.

REM Start in Electron mode
npm run dev

pause
