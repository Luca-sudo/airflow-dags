import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="my_dag_name",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="*/1 * * * *",
    ):

    op1 = EmptyOperator(task_id="task1")
    op2 = EmptyOperator(task_id="task2")
    op3 = EmptyOperator(task_id="task3")
    op4 = PostgresOperator(
            task_id="create_table",
            postgres_conn_id="postgres",
            sql="""
            CREATE TABLE IF NOT EXISTS some_table(
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
            """
            )
    op5 = PostgresOperator(
            task_id="insert_names",
            postgres_conn_id="postgres",
            sql="""
            INSERT INTO some_table (name) VALUES ('reinhard');
            """
            )
    op1 >> op3 >> op4 >> op2
    op1 >> op2
    op4 >> op5
