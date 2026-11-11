
import json
from pathlib import Path

OUTPUT = Path("monitoring/grafana/dashboards")
OUTPUT.mkdir(parents=True, exist_ok=True)

PROM = {"type": "prometheus", "uid": "prometheus"}


def panel(title, expression, x, y, unit="short", kind="timeseries"):
    return {
        "id": 1,
        "title": title,
        "type": kind,
        "datasource": PROM,
        "gridPos": {"x": x, "y": y, "w": 12, "h": 8},
        "targets": [
            {
                "refId": "A",
                "expr": expression,
                "legendFormat": "{{version}} {{le}}",
            }
        ],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
            },
            "overrides": [],
        },
        "options": {
            "legend": {
                "displayMode": "list",
                "placement": "bottom",
            }
        } if kind == "timeseries" else {},
    }


def dashboard(uid, title, specs):
    panels = []

    for index, spec in enumerate(specs):
        name, query, unit = spec
        x = (index % 2) * 12
        y = (index // 2) * 8

        p = panel(name, query, x, y, unit)
        p["id"] = index + 1
        panels.append(p)

    data = {
        "uid": uid,
        "title": title,
        "tags": ["fraud", "mlops", "monitoring"],
        "timezone": "browser",
        "schemaVersion": 41,
        "version": 1,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "panels": panels,
    }

    filename = OUTPUT / f"{uid}.json"
    filename.write_text(
        json.dumps(data, indent=2),
        encoding="utf-8",
    )
    print(f"Created: {filename}")


dashboard(
    "fraud-scoring",
    "Fraud Detection - Transaction Scoring",
    [
        (
            "Transactions per second",
            "rate(fraud_scoring_processed_total[1m])",
            "ops",
        ),
        (
            "Average scoring latency",
            "rate(fraud_scoring_duration_seconds_sum[1m]) "
            "/ clamp_min(rate(fraud_scoring_duration_seconds_count[1m]), 1e-9)",
            "s",
        ),
        (
            "Fraud fraction",
            "rate(fraud_scoring_fraud_total[1m]) "
            "/ clamp_min(rate(fraud_scoring_processed_total[1m]), 1e-9)",
            "percentunit",
        ),
        (
            "Score histogram - cumulative buckets",
            "sum by (le) (increase(fraud_scoring_score_bucket[5m]))",
            "short",
        ),
        (
            "Model loads by version",
            "fraud_model_reload_total",
            "short",
        ),
        (
            "Seconds since last transaction",
            "time() - fraud_scoring_last_processed_timestamp_seconds",
            "s",
        ),
    ],
)

dashboard(
    "fraud-writer",
    "Fraud Detection - Results Writer",
    [
        (
            "Database writes per second",
            "rate(fraud_writer_written_total[1m])",
            "ops",
        ),
        (
            "Average database write latency",
            "rate(fraud_writer_write_seconds_sum[1m]) "
            "/ clamp_min(rate(fraud_writer_write_seconds_count[1m]), 1e-9)",
            "s",
        ),
        (
            "Detected fraud transactions",
            "fraud_writer_fraud_total",
            "short",
        ),
        (
            "Saved score histogram - cumulative buckets",
            "sum by (le) (increase(fraud_writer_score_bucket[5m]))",
            "short",
        ),
    ],
)

dashboard(
    "fraud-system",
    "Fraud Detection - System Monitoring",
    [
        (
            "CPU utilization",
            "1 - avg(rate(node_cpu_seconds_total{mode=\"idle\"}[1m]))",
            "percentunit",
        ),
        (
            "Memory utilization",
            "1 - node_memory_MemAvailable_bytes "
            "/ node_memory_MemTotal_bytes",
            "percentunit",
        ),
        (
            "Network receive bytes per second",
            "sum(rate(node_network_receive_bytes_total[1m]))",
            "Bps",
        ),
        (
            "Disk read bytes per second",
            "sum(rate(node_disk_read_bytes_total[1m]))",
            "Bps",
        ),
        (
            "Disk write bytes per second",
            "sum(rate(node_disk_written_bytes_total[1m]))",
            "Bps",
        ),
        (
            "Filesystem space used",
            "1 - node_filesystem_avail_bytes "
            "/ node_filesystem_size_bytes",
            "percentunit",
        ),
    ],
)
