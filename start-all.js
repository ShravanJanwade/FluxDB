/**
 * FluxDB - Cross-Platform Start Script
 * 
 * This Node.js script starts both the FluxDB backend server
 * and the FluxDB Studio Electron frontend
 * 
 * Usage: node start-all.js
 */

const { spawn, exec } = require('child_process');
const path = require('path');
const fs = require('fs');

const ROOT_DIR = __dirname;
const FLUXDB_DIR = path.join(ROOT_DIR, 'fluxdb');
const STUDIO_DIR = path.join(ROOT_DIR, 'fluxdb-studio');

const isWindows = process.platform === 'win32';

console.log('\n========================================');
console.log('  FluxDB - Starting All Services');
console.log('========================================\n');

// Check requirements
async function checkRequirements() {
  console.log('Checking requirements...\n');
  
  // Check for Cargo (Rust)
  try {
    await execPromise('cargo --version');
    console.log('✓ Cargo (Rust) found');
  } catch {
    console.error('✗ Cargo not found! Please install Rust from https://rustup.rs');
    process.exit(1);
  }
  
  // Check for Node.js
  try {
    await execPromise('node --version');
    console.log('✓ Node.js found');
  } catch {
    console.error('✗ Node.js not found!');
    process.exit(1);
  }
  
  // Check for npm
  try {
    await execPromise('npm --version');
    console.log('✓ npm found');
  } catch {
    console.error('✗ npm not found!');
    process.exit(1);
  }
  
  console.log('');
}

function execPromise(command) {
  return new Promise((resolve, reject) => {
    exec(command, (error, stdout, stderr) => {
      if (error) reject(error);
      else resolve(stdout);
    });
  });
}

async function buildBackend() {
  console.log('[1/4] Building FluxDB server...');
  
  return new Promise((resolve, reject) => {
    const build = spawn('cargo', ['build', '--release'], {
      cwd: FLUXDB_DIR,
      stdio: 'inherit',
      shell: isWindows
    });
    
    build.on('close', code => {
      if (code === 0) resolve();
      else reject(new Error(`Build failed with code ${code}`));
    });
  });
}

function startBackend() {
  console.log('[2/4] Starting FluxDB server...');
  
  const server = spawn('cargo', ['run', '--release', '--bin', 'fluxdb'], {
    cwd: FLUXDB_DIR,
    stdio: ['ignore', 'pipe', 'pipe'],
    shell: isWindows,
    detached: !isWindows
  });
  
  server.stdout.on('data', data => {
    const line = data.toString().trim();
    if (line) console.log(`[FluxDB] ${line}`);
  });
  
  server.stderr.on('data', data => {
    const line = data.toString().trim();
    if (line) console.error(`[FluxDB] ${line}`);
  });
  
  return server;
}

async function installFrontendDeps() {
  console.log('[3/4] Installing frontend dependencies...');
  
  // Check if node_modules exists
  const nodeModules = path.join(STUDIO_DIR, 'node_modules');
  if (fs.existsSync(nodeModules)) {
    console.log('Dependencies already installed, skipping...');
    return;
  }
  
  return new Promise((resolve, reject) => {
    const install = spawn('npm', ['install'], {
      cwd: STUDIO_DIR,
      stdio: 'inherit',
      shell: isWindows
    });
    
    install.on('close', code => {
      if (code === 0) resolve();
      else reject(new Error(`npm install failed with code ${code}`));
    });
  });
}

function startFrontend() {
  console.log('[4/4] Starting FluxDB Studio...\n');
  console.log('========================================');
  console.log('  FluxDB server running on :8086');
  console.log('  FluxDB Studio starting...');
  console.log('========================================\n');
  
  const frontend = spawn('npm', ['run', 'dev'], {
    cwd: STUDIO_DIR,
    stdio: 'inherit',
    shell: isWindows
  });
  
  return frontend;
}

async function main() {
  let serverProcess = null;
  
  try {
    await checkRequirements();
    await buildBackend();
    
    serverProcess = startBackend();
    
    // Wait for server to start
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    await installFrontendDeps();
    
    const frontend = startFrontend();
    
    // Handle cleanup
    process.on('SIGINT', () => {
      console.log('\nShutting down...');
      if (serverProcess) {
        if (isWindows) {
          exec(`taskkill /pid ${serverProcess.pid} /f /t`);
        } else {
          process.kill(-serverProcess.pid);
        }
      }
      process.exit(0);
    });
    
    frontend.on('close', () => {
      if (serverProcess) {
        if (isWindows) {
          exec(`taskkill /pid ${serverProcess.pid} /f /t`);
        } else {
          process.kill(-serverProcess.pid);
        }
      }
    });
    
  } catch (error) {
    console.error(`\nError: ${error.message}`);
    if (serverProcess) {
      serverProcess.kill();
    }
    process.exit(1);
  }
}

main();
