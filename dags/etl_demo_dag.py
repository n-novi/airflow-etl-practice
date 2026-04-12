from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os

RAW_PATH = "/opt/airflow/data/raw/data.csv"
OUT_PATH = "/opt/airflow/data/processed/result.csv"


# -------------------
# EXTRACT
# -------------------
def extract():
    os.makedirs("/opt/airflow/data/raw", exist_ok=True)

    df = pd.DataFrame({
        "name": ["alice", "bob", "charlie"],
        "age": [23, 30, 35]
    })

    df.to_csv(RAW_PATH, index=False)
    print("Extract done")


# -------------------
# TRANSFORM
# -------------------
def transform():
    os.makedirs("/opt/airflow/data/processed", exist_ok=True)

    df = pd.read_csv(RAW_PATH)
    df["age_plus_10"] = df["age"] + 10

    df.to_csv(OUT_PATH, index=False)
    print("Transform done")


# -------------------
# LOAD
# -------------------
def load():
    df = pd.read_csv(OUT_PATH)
    print("FINAL RESULT:")
    print(df)


# -------------------
# DAG
# -------------------
with DAG(
    dag_id="etl_demo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable=load
    )

    t1 >> t2 >> t3