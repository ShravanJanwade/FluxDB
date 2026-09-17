/**
 * Local browser development launcher. Usage: node start-all.js [--verify]
 * --verify starts both services, checks readiness, and shuts them down.
 */
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const net = require('node:net');
const crypto = require('node:crypto');

const root = __dirname;
if (fs.existsSync(path.join(root, '.env'))) process.loadEnvFile(path.join(root, '.env'));
const backend = path.join(root, 'fluxdb');
const studio = path.join(root, 'fluxdb-studio');
const windows = process.platform === 'win32';
const installedCargo = path.join(os.homedir(), '.cargo', 'bin', windows ? 'cargo.exe' : 'cargo');
const cargo = fs.existsSync(installedCargo) ? installedCargo : 'cargo';
const address = process.env.FLUXDB_ADDR || '127.0.0.1:8086';
const apiUrl = new URL('http://' + address);
if (['0.0.0.0', '[::]'].includes(apiUrl.hostname)) apiUrl.hostname = '127.0.0.1';
const port = Number(process.env.FLUXDB_STUDIO_PORT || 5173);
const webUrl = 'http://127.0.0.1:' + port;
const children = new Set();
let stopping = false;

function start(command, args, cwd, env = process.env, shell = false) {
  const child = spawn(command, args, { cwd, env, shell, stdio: 'inherit', windowsHide: true });
  children.add(child);
  child.on('error', error => { console.error(error.message); void stop(1); });
  child.on('exit', () => children.delete(child));
  return child;
}
function run(command, args, cwd, shell = false) {
  return new Promise((resolve, reject) => {
    const child = start(command, args, cwd, process.env, shell);
    child.once('error', reject);
    child.once('exit', code => code === 0 ? resolve() : reject(new Error(command + ' exited with ' + code)));
  });
}
async function stop(code = 0) {
  if (stopping) return;
  stopping = true;
  await Promise.all([...children].map(child => new Promise(resolve => {
    child.once('exit', resolve);
    child.kill('SIGTERM');
    const timer = setTimeout(() => { child.kill('SIGKILL'); resolve(); }, 5000);
    timer.unref();
  })));
  process.exitCode = code;
}
async function freePort(host, value) {
  await new Promise((resolve, reject) => {
    const socket = net.createServer();
    socket.once('error', () => reject(new Error(host + ':' + value + ' is already in use. Stop the existing service first.')));
    socket.listen(value, host, () => socket.close(resolve));
  });
}
async function ready(url, child) {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (child.exitCode !== null || stopping) throw new Error('A service exited before it was ready.');
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(1000) });
      if (response.ok) return;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  throw new Error('Timed out waiting for ' + url);
}
/**
 * A development-only session secret, persisted under .runtime so restarting the
 * launcher does not sign you out. A deployment sets FLUXDB_SESSION_SECRET
 * itself; without either, the server generates an ephemeral one and says so.
 */
function developmentSecret() {
  const file = path.join(root, '.runtime', 'dev-session-secret');
  try {
    if (fs.existsSync(file)) return fs.readFileSync(file, 'utf8').trim();
    const secret = crypto.randomBytes(32).toString('hex');
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, secret, { mode: 0o600 });
    return secret;
  } catch {
    return crypto.randomBytes(32).toString('hex');
  }
}
async function main() {
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('FLUXDB_STUDIO_PORT must be a valid port.');
  await freePort('127.0.0.1', Number(apiUrl.port || 80));
  await freePort('127.0.0.1', port);
  console.log('Building the Rust server...');
  await run(cargo, ['build', '--release', '--locked', '-p', 'fluxdb-server'], backend);
  if (!fs.existsSync(path.join(studio, 'node_modules', 'vite', 'bin', 'vite.js'))) {
    console.log('Installing browser dependencies from the lockfile...');
    await run('npm', ['ci'], studio, windows);
  }
  if (stopping) return;
  const server = start(path.join(backend, 'target', 'release', windows ? 'fluxdb.exe' : 'fluxdb'), [], backend, {
    ...process.env,
    FLUXDB_SESSION_SECRET: process.env.FLUXDB_SESSION_SECRET || developmentSecret(),
  });
  await ready(new URL('/health', apiUrl), server);
  const web = start(process.execPath, [path.join(studio, 'node_modules', 'vite', 'bin', 'vite.js'), '--host', '127.0.0.1', '--port', String(port), '--strictPort'], studio, { ...process.env, FLUXDB_PROXY_TARGET: apiUrl.origin });
  await ready(webUrl, web);
  server.once('exit', code => { if (!stopping) void stop(code || 1); });
  web.once('exit', code => { if (!stopping) void stop(code || 0); });
  console.log([
    '',
    '  FluxDB is running.',
    '',
    '  Console    ' + webUrl,
    '  Demo       ' + webUrl + '/login?demo=1   (one click, no account needed)',
    '  Docs       ' + webUrl + '/docs',
    '  API        ' + apiUrl.origin,
    '',
    '  Press Ctrl+C to stop both services.',
    '',
  ].join('\n'));
  if (process.argv.includes('--verify')) {
    const health = await fetch(webUrl + '/api/v1/health');
    if (!health.ok || (await health.json()).status !== 'ok') throw new Error('Browser proxy health check failed.');
    // The control plane serves accounts and the demo, so reporting success
    // without it would be misleading.
    const config = await fetch(webUrl + '/api/cloud/config');
    if (!config.ok) throw new Error('Control plane is not reachable through the browser proxy.');
    const store = (await config.json()).control_plane;
    if (!store) throw new Error('Control plane did not report a metadata store.');
    console.log('PASS: server, browser, API proxy, and control plane (' + store + ') readiness');
    await stop();
  }
}
process.once('SIGINT', () => void stop());
process.once('SIGTERM', () => void stop());
main().catch(async error => { console.error(error.message); await stop(1); });
