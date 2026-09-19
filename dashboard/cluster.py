"""Read Spark standalone master status from its Web UI JSON endpoint.

The master serves a machine-readable snapshot at `<ui>/json/` (default UI
`http://localhost:8080`, mapped by docker-compose). No SSH, no extra deps —
just stdlib urllib. Pure parsing lives here so it can be tested without a
running cluster; the Streamlit page (pages/2_Cluster_Status.py) only renders.
"""
import json
import time
import urllib.request

DEFAULT_UI = "http://localhost:8080"


def fetch_master_json(ui_url, timeout=3.0):
    """GET `<ui_url>/json/` and return the decoded dict. Raises on failure."""
    url = ui_url.rstrip("/") + "/json/"
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def parse_master(data):
    """Normalize the master JSON into a flat dict the page renders.

    Every field is read with a default so an older/newer Spark schema (or an
    empty reply) degrades gracefully instead of crashing the page.
    """
    workers = [
        {
            "host": w.get("host", "?"),
            "port": w.get("port", 0),
            "state": w.get("state", "?"),
            "cores": w.get("cores", 0),
            "cores_used": w.get("coresused", 0),
            "memory": w.get("memory", 0),
            "memory_used": w.get("memoryused", 0),
            "lastheartbeat": w.get("lastheartbeat", 0),
        }
        for w in data.get("workers", [])
    ]
    apps = [
        {
            "id": a.get("id", "?"),
            "name": a.get("name", "?"),
            "cores": a.get("cores", 0),
            "memory_per_executor": a.get("memoryperexecutor", 0),
            "state": a.get("state", "?"),
            "user": a.get("user", "?"),
        }
        for a in data.get("activeapps", [])
    ]
    return {
        "status": data.get("status", "?"),
        "url": data.get("url", ""),
        "workers_alive": data.get("aliveworkers", sum(w["state"] == "ALIVE" for w in workers)),
        "workers_total": len(workers),
        "cores": data.get("cores", 0),
        "cores_used": data.get("coresused", 0),
        "memory": data.get("memory", 0),
        "memory_used": data.get("memoryused", 0),
        "workers": workers,
        "apps": apps,
    }


def distinct_hosts(workers):
    """Số máy vật lý riêng biệt (theo host IP) trong các worker còn sống.

    Bằng chứng phân tán cho demo: >=2 host khác nhau = chạy trên nhiều máy
    thật, không phải nhiều tiến trình trên một máy.
    """
    return len({w["host"] for w in workers if w.get("state") == "ALIVE"})


def fmt_mb(mb):
    """Human-readable size from a megabyte count (Spark's memory unit)."""
    if mb >= 1024:
        return f"{mb / 1024:.1f} GB"
    return f"{mb} MB"


def heartbeat_age(last_ms, now_ms=None):
    """Whole seconds since a worker's last heartbeat (epoch ms)."""
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    return max(0, (now_ms - last_ms) // 1000)
