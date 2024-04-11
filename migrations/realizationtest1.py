from airflow import DAG
from airflow.operators import DummyOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
from airflow.utils.task_group import TaskGroup

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow"
}

with DAG(
    dag_id="Sprin4_Task2",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
) as dag:

    # Вам необходимо заполнить только эту секцию с сенсором
    begin = DummyOperator(task_id="begin")
    with TaskGroup(group_id='group1') as tg1:
        f1 = FileSensor(
            task_id="waiting_for_file_customer_research.csv",
            fs_conn_id="fs_local",
            filepath=str(datetime.now().date()) + "_customer_research.csv",
            poke_interval=5)
        f2 = FileSensor(
            task_id="waiting_for_file_user_order_log.csv",
            fs_conn_id="fs_local",
            filepath=str(datetime.now().date()) + "_user_order_log.csv",
            poke_interval=5)
        f3 = FileSensor(
            task_id="waiting_for_file_user_activity_log.csv",
            fs_conn_id="fs_local",
            filepath=str(datetime.now().date()) + "_user_activity_log.csv",
            poke_interval=5)

    end = DummyOperator(task_id="end")
    begin >> tg1 >> end
