# -*- coding: utf-8 -*-

import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

fake = Faker('ru_RU')
load_dotenv(dotenv_path="C:/Users/Дом/Desktop/Стажировка/Инд. проект/.env")

# Источник БД полных имен https://ngodata.ru/dataset/russiannames   
df_names = pd.read_json(os.getenv("PATH_NAMES"), lines= True)
df_midnames = pd.read_json(os.getenv("PATH_MIDNAMES"), lines = True)
df_surnames = pd.read_json(os.getenv("PATH_SURNAMES"), lines = True)


# Выберем частые значения и определеные столбцы
df_names = df_names[(df_names['text'] != 'A. ') & 
                    (df_names['text'] != 'A.') & 
                    (df_names['text'] != ' ') &
                    (df_names['num']>5000)].iloc[:,[0,5]]

df_midnames = df_midnames[(df_midnames['text'] != 'A. ') & 
                          (df_midnames['text'] != ' ') &
                          (df_midnames['num']>5000)].iloc[:,[0,6]]

df_surnames = df_surnames[(df_surnames['text'] != 'A. ') & 
                          (df_surnames['text'] != ' ') &
                          (df_surnames['num']>5000)].iloc[:,[0,4]]


#  Разделим по гендеру для склеивания имен фамилий и отчеств
df_names_male = df_names[df_names['gender'] == 'm']['text']
df_midnames_male = df_midnames[df_midnames['gender'] == 'm']['text']
df_surnames_male = df_surnames[df_surnames['gender'] == 'm']['text']

df_names_female = df_names[df_names['gender'] == 'f']['text']
df_midnames_female = df_midnames[df_midnames['gender'] == 'f']['text']
df_surnames_female = df_surnames[df_surnames['gender'] == 'f']['text']


# Сделаем случайную выборку по частям имен в размере 100 для каждого гендера
names_male = df_names_male.sample(100, replace = True).to_list()
midnames_male = df_midnames_male.sample(100, replace = True).to_list()
surnames_male = df_surnames_male.sample(100, replace = True).to_list()


names_female = df_names_female.sample(100, replace = True).to_list()
midnames_female = df_midnames_female.sample(100, replace = True).to_list()
surnames_female = df_surnames_female.sample(100, replace = True).to_list()


# Обьединим значения по полам
first_names = names_male + names_female
mid_names = midnames_male + midnames_female
second_names = surnames_male + surnames_female 


# Сгенерируем id клиентов для таблицы Clients
client_num= list(range(0,100))


# Сгенерируем полные имена клиентов
client_name = [' '.join([first_names[i], mid_names[i], second_names[i]]) for i in client_num]



# Сгенерируем доходы клиентов
client_revenue = [round(random.randint(30000, 500000),-3) for _ in client_num]

df_clients = pd.DataFrame({'client_name':client_name,
                           'client_revenue':client_revenue})

# Сгенерируем 200 заказов продуктов
usage_id= list(range(0,200))


# Выберем диапазон дат
start_date = datetime(2025, 5, 1, hour=0, minute=0, second=0)
end_date = datetime(2025, 5, 31, hour=23, minute=59, second=59)

usage_date = [start_date]

for _ in range(199):
    rand_date = start_date + timedelta(minutes = random.randint(1, 44630))
    usage_date.append(rand_date)
    


# Сгенерируем id клиентов для таблицы Product_usage
client_id= [random.randint(1, 100)for _ in range(200)]



# Сгенерируем список продуктов
products = pd.DataFrame(['Кредит под залог авто', 
                         'Ипотечный кредит', 
                         'Вклад 3 месяца', 
                         'Вклад 6 месяцев', 
                         'Вклад 12 месяцев',
                         'Кредитная карта',
                         'Дебетовая карта',
                         'Потребительский кредит',
                         'Страхование жизни',
                         'Страхование авто',
                         'Страхование недвижимости']).sample(200,replace=True)

products.index = pd.RangeIndex(start=0,stop=200)
products = products[0].to_list()

df_product_usage = pd.DataFrame({'client_id':client_id,
                                 'product':products,
                                 'usage_date':usage_date
                                 })


df_clients.to_csv(r"etl/csv/clients.csv", index=False, sep=";", encoding="utf-8-sig")
df_product_usage.to_csv(r"etl/csv/product_usage.csv", index=False, sep=";", encoding="utf-8-sig")