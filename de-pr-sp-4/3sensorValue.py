from airflow import DAG
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.sql import (
    SQLCheckOperator,
    SQLValueCheckOperator,
)

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "postgres_conn_id": "postgres_default"
}

def check_success_insert_user_order_log(context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    result1 = hook.get_first("SELECT COUNT(*) FROM user_order_log WHERE customer_id IS NULL;")
    if result1[0] == 0:
        print("Check passed: customer_id fields do not contain NULL")
    else:
        print("Check failed: NULL values found in customer_id fields")
 
def check_failure_insert_user_order_log(context):
    print("Check failed: an error occurred while executing the operator")

def check_success_insert_user_activity_log(context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    result2 = hook.get_first("SELECT COUNT(*) FROM user_activity_log WHERE customer_id IS NULL;")
    if result2[0] == 0:
        print("Check passed: customer_id fields do not contain NULL")
    else:
        print("Check failed: NULL values found in customer_id fields")
 
def check_failure_insert_user_activity_log(context):
    print("Check failed: an error occurred while executing the operator")

def check_null_customer_id(context):
    hook = PostgresHook(postgres_conn_id='postgres_default') 
    result1 = hook.get_first("SELECT COUNT(DISTINCT customer_id) FROM user_order_log;")
    result2 = hook.get_first("SELECT COUNT(DISTINCT customer_id) FROM user_activity_log;")
    if result1[0] == 0 and result2[0] == 0:
        print("Check passed: customer_id fields do not contain NULL")
    else:
        print("Check failed: NULL values found in customer_id fields")

def check_failure_check_null_customer_id(context):
    print("Check failed: an error occurred while executing the operator")

with DAG(
    dag_id="Sprin4_Task1",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
) as dag:

sql_check = SQLCheckOperator(
    task_id="user_order_log_isNull",
    sql="user_order_log_isNull_check.sql",
    on_success_callback=check_success_insert_user_order_log,
    on_failure_callback=check_failure_insert_user_order_log
)

sql_check2 = SQLCheckOperator(
    task_id="user_activity_log_isNull",
    sql="user_activity_log_isNull_check.sql",
    on_success_callback=check_success_insert_user_activity_log,
    on_failure_callback=check_failure_insert_user_activity_log
)

sql_check3 = SQLValueCheckOperator(
    task_id='check_null_customer_id',
    sql="check_null_customer_id.sql",
    pass_value='3',
    on_success_callback=check_null_customer_id,
    on_failure_callback=check_failure_check_null_customer_id
)

check_null_customer_id >> sql_check >> sql_check2 >> sql_check3