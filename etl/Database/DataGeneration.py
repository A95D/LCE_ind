# -*- coding: utf-8 -*-

import pandas as pd
import random
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

class DataGenerator:

    def __init__(self):
        os.chdir('C:/Users/Дом/Desktop/Стажировка/Инд. проект')
        load_dotenv()

        # Загрузка исходных данных
        self.df_names = pd.read_json(os.getenv("PATH_NAMES"), lines=True)
        self.df_midnames = pd.read_json(os.getenv("PATH_MIDNAMES"), lines=True)
        self.df_surnames = pd.read_json(os.getenv("PATH_SURNAMES"), lines=True)

        self.prepare_name_data()

    def prepare_name_data(self):
        # Очистка и фильтрация данных
        self.df_names = self.df_names[(self.df_names['text'] != 'A. ') &
                                      (self.df_names['text'] != 'A.') &
                                      (self.df_names['text'] != ' ') &
                                      (self.df_names['num'] > 5000)].iloc[:, [0, 5]]

        self.df_midnames = self.df_midnames[(self.df_midnames['text'] != 'A. ') &
                                            (self.df_midnames['text'] != ' ') &
                                            (self.df_midnames['num'] > 5000)].iloc[:, [0, 6]]

        self.df_surnames = self.df_surnames[(self.df_surnames['text'] != 'A. ') &
                                            (self.df_surnames['text'] != ' ') &
                                            (self.df_surnames['num'] > 5000)].iloc[:, [0, 4]]

    def generate_clients(self, num_clients=100):
        # Разделение по гендеру
        df_names_male = self.df_names[self.df_names['gender'] == 'm']['text']
        df_midnames_male = self.df_midnames[self.df_midnames['gender'] == 'm']['text']
        df_surnames_male = self.df_surnames[self.df_surnames['gender'] == 'm']['text']

        df_names_female = self.df_names[self.df_names['gender'] == 'f']['text']
        df_midnames_female = self.df_midnames[self.df_midnames['gender'] == 'f']['text']
        df_surnames_female = self.df_surnames[self.df_surnames['gender'] == 'f']['text']

        # Сделаем случайную выборку по частям имен в размере 100 для каждого гендера
        names_male = df_names_male.sample(num_clients, replace=True).to_list()
        midnames_male = df_midnames_male.sample(num_clients, replace=True).to_list()
        surnames_male = df_surnames_male.sample(num_clients, replace=True).to_list()

        names_female = df_names_female.sample(num_clients, replace=True).to_list()
        midnames_female = df_midnames_female.sample(num_clients, replace=True).to_list()
        surnames_female = df_surnames_female.sample(num_clients, replace=True).to_list()

        # Обьединим значения по гендеру
        first_names = names_male + names_female
        mid_names = midnames_male + midnames_female
        second_names = surnames_male + surnames_female
            
        # Сгенерируем id клиентов для таблицы Clients
        client_num = list(range(0, num_clients))
        
        # Сгенерируем полные имена клиентов
        client_name = [' '.join([first_names[i], mid_names[i], second_names[i]]) for i in client_num]
        
        # Сгенерируем доходы клиентов
        client_revenue = [round(random.randint(30000, 500000), -3) for _ in client_num]

        df_clients = pd.DataFrame({'client_name': client_name,
                                   'client_revenue': client_revenue})
        
        df_clients.to_csv(r"etl/Database/csv/clients.csv", index=False, sep=",", encoding="utf-8-sig")
        
    def generate_product_usage(self, num_records=200):
        # Даты
        start_date = datetime(2025, 5, 1, hour=0, minute=0, second=0)
        end_date = datetime(2025, 5, 31, hour=23, minute=59, second=59)
        delta_minutes = int((end_date - start_date).total_seconds() // 60)

        usage_date = [start_date]
        for _ in range(num_records - 1):
            rand_date = start_date + timedelta(minutes = random.randint(1, delta_minutes))
            usage_date.append(rand_date)

        # Сгенерируем id клиентов для таблицы Product_usage
        client_id = [random.randint(1, 100) for _ in range(num_records)]

        # Сгенерируем список продуктов
        products_list = [
            'Кредит под залог авто',
            'Ипотечный кредит',
            'Вклад 3 месяца',
            'Вклад 6 месяцев',
            'Вклад 12 месяцев',
            'Кредитная карта',
            'Дебетовая карта',
            'Потребительский кредит',
            'Страхование жизни',
            'Страхование авто',
            'Страхование недвижимости'
        ]

        products = pd.DataFrame(products_list).sample(num_records, replace=True)
        products.index = pd.RangeIndex(start=0, stop=num_records)
        products = products[0].to_list()

        df_product_usage = pd.DataFrame({'client_id': client_id,
                                         'product': products,
                                         'usage_date': usage_date})
        
        df_product_usage.to_csv(r"etl/Database/csv/product_usage.csv", index=False, sep=",", encoding="utf-8-sig")


    def generate_all(self):
        self.generate_clients()
        self.generate_product_usage()
