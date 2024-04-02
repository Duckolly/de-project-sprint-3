# import requests

# url = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/generate_report'
# headers = {
#     'X-Nickname': 'Duckolly',
#     'X-Cohort': '24',
#     'X-Project': 'True',
#     'X-API-KEY': '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
# }
# data = ''
# response = requests.post(url, headers=headers, data=data)
# print(response.status_code)  # вывод кода ответа сервера
# print(response.text) 

# import requests

# url = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/generate_report'
# headers = {
#     'X-Nickname': '{{ Duckolly }}',
#     'X-Cohort': '{{ 24 }}',
#     'X-Project': 'True',
#     'X-API-KEY': '{{ 5f55e6c0-e9e5-4a9c-b313-63c01fc31460 }}'
# }
# data = ''

# response = requests.post(url, headers=headers, data=data)
# if response.status_code == 200:
#     task_id = response.json().get('task_id')
#     print(f'Task ID: {task_id}')
# else:
#     print(f'Error: {response.status_code}')



#     curl --location --request GET https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_report?task_id={{ task_id }} 
# --header 'X-Nickname: 'Duckolly'
# --header 'X-Cohort: '24' 
# --header 'X-Project: 'True'
# --header 'X-API-KEY: '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
    

curl -X GET "https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_report?task_id=MjAyNC0wNC0wMlQwOTozNTo1MwlEdWNrb2xseQ==" ^
 -H "X-Nickname: Duckolly" ^
 -H "X-Cohort: 24" ^
 -H "X-Project: True" ^
 -H "X-API-KEY: 5f55e6c0-e9e5-4a9c-b313-63c01fc31460" ^

curl -X POST "https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/generate_report" ^
 -H "X-Nickname: Duckolly" ^
 -H "X-Cohort: 24" ^
 -H "X-Project: True" ^
 -H "X-API-KEY: 5f55e6c0-e9e5-4a9c-b313-63c01fc31460" ^
--data-raw ""
{"task_id":"MjAyNC0wNC0wMlQwOTozNTo1MwlEdWNrb2xseQ=="}


curl --location --request GET 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_increment?report_id="TWpBeU5DMHdOQzB3TWxRd09Ub3pOVG8xTXdsRWRXTnJiMnhzZVE9PQ=="&date="2024-03-26 00:00:00"' \
 -H "X-Nickname: Duckolly" ^
 -H "X-Cohort: 24" ^
 -H "X-Project: True" ^
 -H "X-API-KEY: 5f55e6c0-e9e5-4a9c-b313-63c01fc31460" ^