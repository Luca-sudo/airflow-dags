import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="my_dag_name",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
    ):

    op1 = EmptyOperator(task_id="task1")
    op2 = EmptyOperator(task_id="task2")
    op3 = EmptyOperator(task_id="task3")
    op4 = EmptyOperator(task_id="task4")
    op1 >> op3 >> op4 >> op2
    op1 >> op2
