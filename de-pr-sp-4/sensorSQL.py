# from airflow import DAG
# from datetime import datetime
# from airflow.operators.sql import SQLCheckOperator

# default_args = {
#     "start_date": datetime(2020, 1, 1),
#     "owner": "airflow",
#     "conn_id": "postgres_default"
# }


# with DAG(
#     dag_id="Sprin4_Task1",
#     schedule_interval="@daily",
#     default_args=default_args,
#     catchup=False
#     ) as dag:

#     # Вам необходимо заполнить только эту секцию
#     sql_check  = SQLCheckOperator(task_id="user_order_log_isNull", sql="user_order_log_isNull_check.sql" , on_success_callback = check_success_insert_user_order_log, on_failure_callback =  check_failure_insert_user_order_log )
#     sql_check2  = SQLCheckOperator(task_id="user_activity_log_isNull", sql="user_activity_log_isNull_check.sql" , on_success_callback = check_success_insert_user_activity_log, on_failure_callback =  check_failure_insert_user_activity_log )

#     sql_check >> sql_check2
        
# def check_success_insert_user_order_log(context):
#     # Подключение к базе данных
#     hook = PostgresHook(postgres_conn_id='postgres_default')
    
#     # Выполнение SQL-запросов
#     result1 = hook.get_first("SELECT COUNT(*) FROM user_order_log WHERE customer_id IS NULL;")
#     result2 = hook.get_first("SELECT COUNT(*) FROM user_activity_log WHERE customer_id IS NULL;")
    
#     # Проверка результатов
#     if result1[0] == 0 and result2[0] == 0:
#         print("Проверка пройдена: поля customer_id не содержат NULL")
#     else:
#         print("Проверка не пройдена: найдены NULL значения в полях customer_id")

# def check_failure_insert_user_activity_log(context):
#     # Обработка неудачного выполнения оператора
#     print("Проверка не пройдена: возникла ошибка при выполнении оператора")
from airflow import DAG
from datetime import datetime
from airflow.operators.sql import SQLCheckOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "conn_id": "postgres_default"
}

def check_success_insert_user_order_log(context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    result1 = hook.get_first("SELECT COUNT(*) FROM user_order_log WHERE customer_id IS NULL;")
    if result1[0] == 0 :
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
    
    sql_check >> sql_check2