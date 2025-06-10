# -*- coding: utf-8 -*-
"""
Created on Sun Jun  1 14:41:37 2025

@author: Дом
"""
import csv
import os
import psycopg2
from dotenv import load_dotenv
from typing import Any, Callable, List, Tuple

#Использую переменные окружения
os.chdir('C:/Users/Дом/Desktop/Стажировка/Инд. проект')
load_dotenv()
db_name = os.getenv("DB_NAME")
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
path_clients = os.getenv("PATH_CLIENTS")
path_usage = os.getenv("PATH_USAGE")


#Функция читает файл и заносит данные в список списков.
#Производится передача списка в функцию для загрузки в базу данных.
def reading_file(path_file: str, loading_function: Callable[[csv.reader, str, Any], None], cur: Any):
    file_name = os.path.basename(path_file)
    
    with open(path_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader) #Пропускаю заголовок
        data = list(reader)
        loading_function(data, file_name, cur)

def loading_clients(clients_info: List[Tuple[Any, Any]], file_name: str, cur: Any) -> None:
    for client in clients_info:
        name, revenue = client  # порядок из CSV
        cur.execute(
            'INSERT INTO staging.clients(name, revenue) VALUES(%s, %s)',
            (name, revenue)
        )
        

def loading_usage(usage_info: List[Tuple[Any, Any, Any]], file_name: str, cur: Any) -> None:
    for product in usage_info:
        client_id, product, usage_date = product  # порядок из CSV
        cur.execute(
            'INSERT INTO staging.product_usage(client_id, product, usage_date) VALUES(%s, %s, %s)',
            (client_id, product, usage_date)
        )

#Подключаюсь к базе данных
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_pass,
    host=db_host
    )

cur = conn.cursor()

#Читаю файлы csv
reading_file(path_file=path_clients, loading_function=loading_clients, cur=cur)
reading_file(path_file=path_usage, loading_function=loading_usage, cur=cur)
print(f"Загрузка из файла '{path_clients}' завершена.")


# Сохраняю изменения и закрываю соединение
conn.commit()
cur.close()
conn.close()
