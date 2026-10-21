from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(
    dag_id="fraud_retrain",
    schedule="2-59/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
)
def fraud_retrain():
    @task
    def prepare():
        """Read labeled ClickHouse data; write no shared state on retries."""
        from mlops.db import client
        from airflow.exceptions import AirflowSkipException
        c = client()
        state = c.query("SELECT next_month FROM fraud.training_state FINAL WHERE id=1").result_rows
        if not state:
            raise AirflowSkipException("Initialize training_state first")
        month = str(state[0][0])[:10]
        # The three months ending in `month` form the window.
        from datetime import date
        from dateutil.relativedelta import relativedelta
        end = date.fromisoformat(month)
        start = end - relativedelta(months=2)
        stop = end + relativedelta(months=1)
        rows = c.query(
            "SELECT * FROM fraud.labeled_transactions "
            "WHERE transaction_time >= {start:Date} AND transaction_time < {stop:Date} "
            "ORDER BY transaction_time",
            parameters={"start": start, "stop": stop},
        )
        import pandas as pd
        frame = pd.DataFrame(rows.result_rows, columns=rows.column_names)
        if frame.empty or pd.to_datetime(frame["transaction_time"]).dt.to_period("M").nunique() < 3:
            raise AirflowSkipException("Incomplete training window")
        path = f"/tmp/fraud_window_{month}.pkl"
        # Single-worker Airflow LocalExecutor only; shared staging needed for distributed workers.
        frame.to_pickle(path)
        return {"path": path, "month": month}

    @task
    def train(info):
        """Read staged window; save candidate only, never change production pointer."""
        import pandas as pd
        from catboost import CatBoostClassifier
        from mlops.features import transform, CATEGORICAL
        from mlops.config import MODEL_DIR
        frame = pd.read_pickle(info["path"])
        months = pd.to_datetime(frame["transaction_time"]).dt.to_period("M")
        test_month = months.max()
        train_df = frame.loc[months < test_month]
        test_df = frame.loc[months == test_month]
        if train_df["is_fraud"].nunique() < 2 :
            raise ValueError("Both train and test need two classes")
        model = CatBoostClassifier(iterations=100, depth=5, learning_rate=0.05,
                                   verbose=False, thread_count=2, random_seed=42,
                                   cat_features=CATEGORICAL)
        model.fit(transform(train_df), train_df["is_fraud"])
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        version = f"fraud_{info['month']}"
        candidate = str(MODEL_DIR / f".candidate_{version}.cbm")
        model.save_model(candidate)
        return {"path": candidate, "version": version, "month": info["month"],
                "test_path": info["path"]}

    @task
    def evaluate(info):
        """Compare candidate and production on the same latest-month holdout."""
        import pandas as pd
        from catboost import CatBoostClassifier
        from sklearn.metrics import average_precision_score
        from mlops.features import transform
        from mlops.model_store import load_current
        frame = pd.read_pickle(info["test_path"])
        months = pd.to_datetime(frame["transaction_time"]).dt.to_period("M")
        test = frame.loc[months == months.max()]
        X, y = transform(test), test["is_fraud"]
        candidate = CatBoostClassifier()
        candidate.load_model(info["path"])
        candidate_ap = float(average_precision_score(y, candidate.predict_proba(X)[:, 1]))
        _, production = load_current()
        prod_ap = None if production is None else float(
            average_precision_score(y, production.predict_proba(X)[:, 1]))
        return {**info, "candidate_ap": candidate_ap, "production_ap": prod_ap,
                "approved": prod_ap is None or candidate_ap >= prod_ap}

    @task
    def finalize(info):
        """Publish only improved candidate; advance month once after a completed decision."""
        from datetime import date
        from dateutil.relativedelta import relativedelta
        from mlops.model_store import publish
        from mlops.db import client
        if info["approved"]:
            publish(info["path"], info["version"])
        # Rejected candidate is processed, not published; advance to avoid blocking future months.
        next_month = date.fromisoformat(info["month"]) + relativedelta(months=1)
        c = client()
        c.command("ALTER TABLE fraud.training_state UPDATE next_month = {m:Date} WHERE id=1",
                  parameters={"m": next_month}, settings={"mutations_sync": 1})
        return {"approved": info["approved"], "candidate_ap": info["candidate_ap"],
                "production_ap": info["production_ap"]}

    finalize(evaluate(train(prepare())))

fraud_retrain()
