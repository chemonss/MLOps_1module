from datetime import datetime
from airflow.decorators import dag, task
import calendar

@dag(
    dag_id="fraud_supply_month",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def fraud_supply_month():
    @task
    def append_month():
        """Clone an existing labeled sample into the next month, once per month."""
        from mlops.db import client
        from dateutil.relativedelta import relativedelta
        import pandas as pd
        import hashlib
        c = client()
        latest = c.query("SELECT max(toStartOfMonth(transaction_time)) "
                         "FROM fraud.labeled_transactions").first_row[0]
        if latest is None:
            raise ValueError("Seed labeled_transactions first")
        target = (pd.Timestamp(latest) + relativedelta(months=1)).date()
        state = c.query("SELECT next_month FROM fraud.training_state FINAL WHERE id=1").result_rows
        if not state or pd.Timestamp(state[0][0]).date() != target:
            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException("Wait for retraining to advance month pointer")
        existing = c.query("SELECT count() FROM fraud.labeled_transactions "
                           "WHERE toStartOfMonth(transaction_time) = {m:Date}",
                           parameters={"m": target}).first_row[0]
        if existing:
            return {"month": str(target), "inserted": 0}
        rows = c.query("SELECT * FROM fraud.labeled_transactions "
                       "WHERE transaction_time < {m:Date} "
                       "ORDER BY cityHash64(toString(transaction_time), merch, cat_id) "
                       "LIMIT 10000", parameters={"m": target})
        if not rows.result_rows:
            raise ValueError("No source rows")
        frame = pd.DataFrame(rows.result_rows, columns=rows.column_names)
        original = pd.to_datetime(frame["transaction_time"])
        days_in_month = calendar.monthrange(target.year, target.month)[1]
        frame["transaction_time"] = [
            datetime(
                target.year,
                target.month,
                min(ts.day, days_in_month),
                ts.hour,
                ts.minute,
                ts.second,
            )
            for ts in original
        ]
        c.insert("fraud.labeled_transactions", frame.values.tolist(),
                 column_names=frame.columns.tolist())
        return {"month": str(target), "inserted": len(frame)}

    @task
    def prune(info):
        """Delete data older than 36 months after successful append."""
        from mlops.db import client
        from dateutil.relativedelta import relativedelta
        from datetime import date
        cutoff = date.fromisoformat(info["month"]) - relativedelta(months=35)
        client().command(
            "ALTER TABLE fraud.labeled_transactions DELETE "
            "WHERE transaction_time < {cutoff:Date}",
            parameters={"cutoff": cutoff}, settings={"mutations_sync": 1})
    prune(append_month())

fraud_supply_month()
