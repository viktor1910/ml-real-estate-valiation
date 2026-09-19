import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))

import cluster


# A trimmed sample of what a Spark standalone master serves at /json/.
# Memory fields are in MB (Spark's unit); one worker is DEAD on purpose.
SAMPLE = {
    "url": "spark://spark-master:7077",
    "status": "ALIVE",
    "aliveworkers": 1,
    "cores": 4,
    "coresused": 2,
    "memory": 4096,
    "memoryused": 1024,
    "workers": [
        {"host": "10.0.0.2", "port": 41001, "state": "ALIVE",
         "cores": 2, "coresused": 2, "memory": 2048, "memoryused": 1024,
         "lastheartbeat": 100_000},
        {"host": "10.0.0.3", "port": 41002, "state": "DEAD",
         "cores": 2, "coresused": 0, "memory": 2048, "memoryused": 0,
         "lastheartbeat": 40_000},
    ],
    "activeapps": [
        {"id": "app-1", "name": "daily_pipeline", "cores": 2,
         "memoryperexecutor": 1024, "state": "RUNNING", "user": "spark"},
    ],
}


def test_parse_master_surfaces_dead_workers():
    # WHY: a DEAD worker is lost capacity the operator must see — the whole
    # point of the page. If parse dropped or mislabeled it, the page would
    # falsely show the cluster as fully healthy.
    p = cluster.parse_master(SAMPLE)
    assert p["workers_total"] == 2
    states = {w["host"]: w["state"] for w in p["workers"]}
    assert states["10.0.0.2"] == "ALIVE"
    assert states["10.0.0.3"] == "DEAD"


def test_parse_master_carries_capacity_and_apps():
    # WHY: cores/memory used-vs-total and running apps are what tells the
    # operator whether the cluster is idle, saturated, or doing work.
    p = cluster.parse_master(SAMPLE)
    assert (p["cores_used"], p["cores"]) == (2, 4)
    assert (p["memory_used"], p["memory"]) == (1024, 4096)
    assert p["status"] == "ALIVE"
    assert len(p["apps"]) == 1
    assert p["apps"][0]["name"] == "daily_pipeline"


def test_parse_master_tolerates_missing_keys():
    # WHY: the /json/ shape varies across Spark versions; a missing key must
    # degrade to a default, never crash the monitoring page.
    p = cluster.parse_master({})
    assert p["workers_total"] == 0
    assert p["cores"] == 0
    assert p["apps"] == []
    assert p["status"] == "?"


def test_distinct_hosts_counts_only_live_physical_machines():
    # WHY: the demo's core claim is "phân tán trên N máy vật lý". That count
    # must be distinct live host IPs — a DEAD worker is not a running machine,
    # and duplicate hosts must not inflate it into a false distributed badge.
    p = cluster.parse_master(SAMPLE)
    # SAMPLE: 10.0.0.2 ALIVE, 10.0.0.3 DEAD -> only 1 live machine.
    assert cluster.distinct_hosts(p["workers"]) == 1
    two = [
        {"host": "10.0.0.2", "state": "ALIVE"},
        {"host": "10.0.0.2", "state": "ALIVE"},  # same box, must not double-count
        {"host": "10.0.0.3", "state": "ALIVE"},
    ]
    assert cluster.distinct_hosts(two) == 2


def test_fmt_mb_reads_as_human_sizes():
    # WHY: Spark reports memory in MB; operators think in GB. Wrong units
    # would make a 2 GB worker look like 2 MB.
    assert cluster.fmt_mb(512) == "512 MB"
    assert cluster.fmt_mb(2048) == "2.0 GB"
    assert cluster.fmt_mb(0) == "0 MB"


def test_heartbeat_age_seconds():
    # WHY: heartbeat age is how you spot a worker that silently stopped
    # reporting; it must be a positive elapsed time, not a raw epoch.
    age = cluster.heartbeat_age(100_000, now_ms=130_000)
    assert age == 30
