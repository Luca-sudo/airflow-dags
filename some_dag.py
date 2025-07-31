import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import random

with DAG(
    dag_id="my_dag_name",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="*/1 * * * *",
    ):

    num_rows = random.randint(1, 100)

    op1 = EmptyOperator(task_id="task1")
    op2 = EmptyOperator(task_id="task2")
    op3 = EmptyOperator(task_id="task3")
    op4 = SQLExecuteQueryOperator(
            task_id="create_table",
            conn_id="postgres",
            sql="""
            CREATE TABLE IF NOT EXISTS some_table(
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
            """
            )
    op5 = SQLExecuteQueryOperator(
            task_id="insert_names",
            conn_id="postgres",
            sql=f"""
            INSERT INTO some_table (name) 
            SELECT 'reinhard' 
            FROM generate_series(1, {num_rows});
            """
            )
    op1 >> op3 >> op4 >> op2
    op1 >> op2
    op4 >> op5
