from datetime import datetime
from airflow.decorators import dag, task

@dag(dag_id="fraud_data_quality", schedule="*/15 * * * *",
     start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1)
def fraud_data_quality():
    @task
    def report():
        """Read labeled dataset, write a unique HTML report; retries overwrite same run report."""
        from airflow.operators.python import get_current_context
        from html import escape
        from mlops.db import client
        from mlops.config import REPORT_DIR
        c = client()
        rules = {
            "Nonempty dataset": "SELECT count() > 0 FROM fraud.labeled_transactions",
            "No null times": "SELECT countIf(isNull(transaction_time)) = 0 FROM fraud.labeled_transactions",
            "Valid target": "SELECT countIf(is_fraud NOT IN (0,1)) = 0 FROM fraud.labeled_transactions",
            "Nonnegative amounts": "SELECT countIf(amount < 0) = 0 FROM fraud.labeled_transactions",
            "Nonempty category": "SELECT countIf(length(trimBoth(cat_id)) = 0) = 0 FROM fraud.labeled_transactions",
            "Both target classes": "SELECT uniqExact(is_fraud) = 2 FROM fraud.labeled_transactions",
        }
        results = [(name, bool(c.query(sql).first_row[0])) for name, sql in rules.items()]
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        run = get_current_context()["run_id"]
        safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in run)
        path = REPORT_DIR / f"dq_{safe}.html"
        rows = "".join(f"<tr><td>{escape(name)}</td><td>{'PASS' if ok else 'FAIL'}</td></tr>"
                       for name, ok in results)
        path.write_text("<!doctype html><html><meta charset='utf-8'>"
                        "<title>Data Quality</title><h1>Fraud Data Quality</h1>"
                        "<table border='1'><tr><th>Rule</th><th>Status</th></tr>"
                        + rows + "</table></html>", encoding="utf-8")
        if not all(ok for _, ok in results):
            raise ValueError(f"Data quality checks failed; report: {path}")
        return str(path)
    report()

fraud_data_quality()
