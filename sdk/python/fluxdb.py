"""Dependency-free FluxDB HTTP client. Timestamps are decimal nanosecond strings."""
import json
from urllib.request import Request, urlopen
from urllib.parse import quote, urlencode
from urllib.error import HTTPError

class FluxDBError(RuntimeError):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status

class FluxDBClient:
    def __init__(self, url="http://127.0.0.1:8086", token=None, timeout=30):
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout

    def request(self, method, path, body=None):
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        data = None if body is None else json.dumps(body, allow_nan=False).encode("utf-8")
        try:
            with urlopen(Request(self.url + path, data=data, headers=headers, method=method), timeout=self.timeout) as response:
                raw = response.read()
                return json.loads(raw) if raw else None
        except HTTPError as error:
            raw = error.read().decode("utf-8", errors="replace")
            try:
                message = json.loads(raw).get("error", raw)
            except (ValueError, AttributeError):
                message = raw
            raise FluxDBError(error.code, message) from error

    @staticmethod
    def path(database):
        return "/api/v1/databases/" + quote(database, safe="")

    def databases(self): return self.request("GET", "/api/v1/databases")
    def create_database(self, database): return self.request("POST", self.path(database))
    def drop_database(self, database): return self.request("DELETE", self.path(database))
    def write(self, database, points): return self.request("POST", self.path(database) + "/points", {"points": points})
    def read(self, database, **filters): return self.request("GET", self.path(database) + "/points?" + urlencode(filters))
    def query(self, database, query): return self.request("POST", self.path(database) + "/query", {"query": query})
    def delete(self, database, measurement, start, end, tags=None, exact=False):
        return self.request("DELETE", self.path(database) + "/points", {"measurement": measurement, "start": str(start), "end": str(end), "tags": tags or {}, "exact": exact})
    def schema(self, database): return self.request("GET", self.path(database) + "/schema")
    def retention(self, database): return self.request("GET", self.path(database) + "/retention")
    def set_retention(self, database, seconds): return self.request("PUT", self.path(database) + "/retention", {"seconds": seconds})
    def flush(self, database): return self.request("POST", self.path(database) + "/flush")
    def compact(self, database): return self.request("POST", self.path(database) + "/compact")
    def export(self, database): return self.request("GET", self.path(database) + "/export")
