SQLValueCheckOperator(
    task_id="check_row_count", # имя задачи
    sql="SELECT COUNT(*) FROM my_table", # запрос для проверки, который считает 
         #количество записей в таблице my_table
    pass_value=20000, # значение, при котором проверку можно считать успешно пройденной 
         #если запрос вернёт значение больше 20000, проверка успешно завершится, если нет,
         #проверка будет признана не успешной.
    tolerance=0.01  # процент отклонения от pass_value в случае, если значение 
        # которое вернёт запрос, будет на 1 процент меньше pass_value 
        # проверка всё равно будет считаться успешно выполненной
        # если отклонение будет больше чем на 1 процент, проверка будет не успешной
)
SQLValueCheckOperator(
    task_id="имя задачи", 
    sql="текст SQL-запроса", 
    pass_value=пороговое значение, 
    tolerance=значение в процентах, допустимое отклонение.  
) 