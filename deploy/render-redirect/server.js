// ============================================================
// Redirect from the old Render address
// ============================================================
// FluxDB moved to Azure Container Apps, but resumes already sent
// out link to fluxdb-demo.onrender.com. Running this on that
// Render service (free plan) sends every request to the same path
// on REDIRECT_TO, so /login lands on the Azure /login. /health stays
// local for Render's health check. No dependencies.
// ============================================================
const http = require('node:http');

const target = (process.env.REDIRECT_TO ?? '').replace(/\/+$/, '');
if (!/^https:\/\/[^/]+$/.test(target)) {
  console.error('Set REDIRECT_TO to the https:// origin to send visitors to, e.g. https://example.com');
  process.exit(1);
}

http
  .createServer((request, response) => {
    if (request.url === '/health') {
      response.writeHead(200, { 'Content-Type': 'text/plain' });
      response.end('ok');
      return;
    }
    // Only ever a path on the target: an absolute-form request URL cannot
    // turn this into a redirect to someone else's site.
    const path = request.url?.startsWith('/') ? request.url : '/';
    response.writeHead(301, {
      Location: target + path,
      // A day rather than forever, so a later move is picked up.
      'Cache-Control': 'public, max-age=86400',
    });
    response.end();
  })
  .listen(Number(process.env.PORT) || 10000, '0.0.0.0', () => {
    console.log(`Redirecting every request to ${target}`);
  });
