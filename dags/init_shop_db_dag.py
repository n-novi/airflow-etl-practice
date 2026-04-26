from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import logging

DATA_PATH = "/opt/airflow/data/db_shop"

default_args = {
    "start_date": datetime(2024, 1, 1),
    "catchup": False
}

def load_csv_to_table(file, table, columns, conn_id):
    try:
        path = f"{DATA_PATH}/{file}"
        df = pd.read_csv(path)

        if df.empty:
            logging.warning(f"[{table}] CSV is empty")
            return

        df = df.where(pd.notnull(df), None)

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()

        cols = ",".join(columns)
        placeholders = ",".join(["%s"] * len(columns))

        sql = f"""
        INSERT INTO {table} ({cols})
        VALUES ({placeholders})
        ON CONFLICT (id) DO NOTHING
        """

        data = []

        for _, row in df.iterrows():
            try:
                cleaned_row = []

                for col in columns:
                    val = row[col]

                    # convert numpy → python types
                    if pd.isna(val):
                        cleaned_row.append(None)
                    elif col in ["id", "order_id", "product_id", "qty", "customer_id"]:
                        cleaned_row.append(int(val))
                    elif col == "price":
                        cleaned_row.append(float(val))
                    elif col == "order_date":
                        cleaned_row.append(str(val))
                    else:
                        cleaned_row.append(val)

                data.append(tuple(cleaned_row))

            except Exception as e:
                logging.warning(f"[{table}] bad row skipped: {row} | {e}")

        if not data:
            logging.warning(f"[{table}] no valid rows")
            return

        cur.executemany(sql, data)
        conn.commit()

        logging.info(f"[{table}] loaded {len(data)} rows")

    except FileNotFoundError:
        logging.error(f"[{table}] file not found: {file}")
        raise

    except Exception as e:
        logging.error(f"[{table}] failed: {e}")
        raise


with DAG(
    dag_id="init_shop_db",
    default_args=default_args,
    schedule=None
) as dag:

    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="shop_db",
        sql="""
        CREATE TABLE IF NOT EXISTS customers (
            id INT PRIMARY KEY,
            name TEXT,
            email TEXT
        );

        CREATE TABLE IF NOT EXISTS products (
            id INT PRIMARY KEY,
            name TEXT,
            price NUMERIC
        );

        CREATE TABLE IF NOT EXISTS orders (
            id INT PRIMARY KEY,
            customer_id INT,
            order_date DATE
        );

        CREATE TABLE IF NOT EXISTS order_items (
            id INT PRIMARY KEY,
            order_id INT,
            product_id INT,
            qty INT,
            price NUMERIC
        );
        """
    )

    load_customers = PythonOperator(
        task_id="load_customers",
        python_callable=load_csv_to_table,
        op_kwargs={
            "file": "customers.csv",
            "table": "customers",
            "columns": ["id", "name", "email"],
            "conn_id": "shop_db"
        }
    )

    load_products = PythonOperator(
        task_id="load_products",
        python_callable=load_csv_to_table,
        op_kwargs={
            "file": "products.csv",
            "table": "products",
            "columns": ["id", "name", "price"],
            "conn_id": "shop_db"
        }
    )

    load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_csv_to_table,
        op_kwargs={
            "file": "orders.csv",
            "table": "orders",
            "columns": ["id", "customer_id", "order_date"],
            "conn_id": "shop_db"
        }
    )

    load_order_items = PythonOperator(
        task_id="load_order_items",
        python_callable=load_csv_to_table,
        op_kwargs={
            "file": "order_items.csv",
            "table": "order_items",
            "columns": ["id", "order_id", "product_id", "qty", "price"],
            "conn_id": "shop_db"
        }
    )

    create_tables >> [
        load_customers,
        load_products,
        load_orders,
        load_order_items
    ]