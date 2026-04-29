from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime


with DAG(
    dag_id="init_sales_mart",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    # 1. CREATE MART TABLE
    create_mart_table = PostgresOperator(
        task_id="create_sales_mart",
        postgres_conn_id="shop_db",
        sql="""
        DROP TABLE IF EXISTS sales_mart;

        CREATE TABLE sales_mart AS
        SELECT
            o.id AS order_id,
            o.order_date,
            c.id AS customer_id,
            c.name AS customer_name,
            c.email,

            COUNT(oi.id) AS items_count,
            SUM(oi.qty * oi.price) AS total_amount

        FROM orders o
        JOIN customers c ON c.id = o.customer_id
        JOIN order_items oi ON oi.order_id = o.id

        GROUP BY
            o.id,
            o.order_date,
            c.id,
            c.name,
            c.email;
        """
    )