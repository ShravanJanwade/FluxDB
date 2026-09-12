/** FluxDB HTTP client for Node.js 20+ and modern browsers. */
export class FluxDBError extends Error {
  constructor(status, message) { super(message); this.name = 'FluxDBError'; this.status = status; }
}
export class FluxDBClient {
  constructor(url = 'http://127.0.0.1:8086', token, timeout = 30000) {
    this.url = url.replace(/\/$/, ''); this.token = token; this.timeout = timeout;
  }
  async request(method, path, body) {
    const response = await fetch(this.url + path, {
      method, headers: { 'Content-Type': 'application/json', ...(this.token ? { Authorization: `Bearer ${this.token}` } : {}) },
      body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(this.timeout),
    });
    const raw = await response.text();
    let value; try { value = raw ? JSON.parse(raw) : null; } catch { throw new FluxDBError(response.status, raw || 'Invalid server response'); }
    if (!response.ok) throw new FluxDBError(response.status, value?.error || `HTTP ${response.status}`);
    return value;
  }
  path(database) { return '/api/v1/databases/' + encodeURIComponent(database); }
  databases() { return this.request('GET', '/api/v1/databases'); }
  createDatabase(db) { return this.request('POST', this.path(db)); }
  dropDatabase(db) { return this.request('DELETE', this.path(db)); }
  write(db, points) { return this.request('POST', this.path(db) + '/points', { points }); }
  read(db, filters = {}) { return this.request('GET', this.path(db) + '/points?' + new URLSearchParams(filters)); }
  query(db, query) { return this.request('POST', this.path(db) + '/query', { query }); }
  delete(db, measurement, start, end, tags = {}, exact = false) { return this.request('DELETE', this.path(db) + '/points', { measurement, start: String(start), end: String(end), tags, exact }); }
  schema(db) { return this.request('GET', this.path(db) + '/schema'); }
  retention(db) { return this.request('GET', this.path(db) + '/retention'); }
  setRetention(db, seconds) { return this.request('PUT', this.path(db) + '/retention', { seconds }); }
  flush(db) { return this.request('POST', this.path(db) + '/flush'); }
  compact(db) { return this.request('POST', this.path(db) + '/compact'); }
  export(db) { return this.request('GET', this.path(db) + '/export'); }
}
