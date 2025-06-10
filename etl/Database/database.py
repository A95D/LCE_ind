# -*- coding: utf-8 -*-

import os
import csv
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from etl.config.logger_config import setup_logger
from typing import Any, Callable, List, Tuple

os.chdir('C:/Users/Дом/Desktop/Стажировка/Инд. проект')
load_dotenv()

logger = setup_logger('config/etl.log')


class DBExtractor:

    def __init__(self):
        dbname = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASS')
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')

        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        self.engine = create_engine(conn_str)

    
    def seq_restart(self, name):
        #сброс последовательности
        with self.engine.begin() as connection:
            connection.execute(text(f"ALTER SEQUENCE {name} RESTART WITH 1"))
            logger.info("Последовательность {name} сброшена")
    
    def reading_file(self, path_file: str, loading_function: Callable[[csv.reader, str, Any], None]):
        file_name = os.path.basename(path_file)
        
        with open(path_file, 'r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file,delimiter=',')
            next(reader) #Пропускаю заголовок
            data = list(reader)
            loading_function(data, file_name)
            logger.info("Загрузка файла {file_name} на слой staging успешно завершена")

    def loading_clients(self, clients_info: List[Tuple[Any, Any]], file_name: str) -> None:
        if not clients_info:
            logger.info(f"{file_name} пустой - пропустить загрузку")
            return
        df = pd.DataFrame(clients_info, columns=["name", "revenue"])
        
        with self.engine.begin() as connection:
          df.to_sql(
              name='clients',
              con=connection,
              schema='staging',
              if_exists='append',
              index=False,
              method='multi'
              )  
            
          logger.info("Датафрейм загружен на слой staging.")
          
    def loading_usage(self, usage_info: List[Tuple[Any, Any, Any]], file_name: str) -> None:
        if not usage_info:
            logger.info(f"{file_name} пустой - пропустить загрузку")
            return
        df = pd.DataFrame(usage_info, columns=["client_id", "product", "usage_date"])
        
        with self.engine.begin() as connection:
            df.to_sql(
                name='product_usage',
                con=connection,
                schema='staging',
                if_exists='append',
                index=False,
                method='multi'
                )
        
            logger.info("Датафрейм загружен на слой staging.")
            
    def create_table(self, name: str):
        # Создание таблиц clients, product_usage с проверкой на существование
        with self.engine.begin() as connection:
            if name == 'clients':
                connection.execute(text('''
                    CREATE TABLE IF NOT EXISTS staging.clients (
                	id serial,
                	name varchar(200) NOT NULL,
                	revenue int,
                	CONSTRAINT clients_pkey PRIMARY KEY (id))'''))
            elif name == 'product_usage':
                connection.execute(text('''
                    CREATE TABLE IF NOT EXISTS staging.product_usage (
                	id serial,
                	client_id int NOT NULL,
                	product varchar(200) NOT NULL,
                	usage_date date NOT NULL,
                	CONSTRAINT product_usage_pkey PRIMARY KEY (id),
                    CONSTRAINT product_usage_client_id_fkey FOREIGN KEY (client_id) REFERENCES staging.clients(id))'''))
            else:
                connection.execute(text('''
                    CREATE TABLE if not exists data_mart.client_segments (
                	client_id int NOT NULL,
                	client_name varchar(200) NOT NULL,
                	client_revenue int,
                	usage_count int,
                	client_segment varchar(50),
                	last_usage_date date,
                	CONSTRAINT client_segments_pkey PRIMARY KEY (client_id),
                    CONSTRAINT client_segments_client_id_fkey FOREIGN KEY (client_id) REFERENCES staging.clients(id))'''))
    
            logger.info("Таблица {name} успешно создана")
        
        
    def load_sql(self, path: str) -> str:
        # Чтение SQL-запроса из файла
        full_path = os.path.join(os.path.dirname(__file__), path)
        with open(full_path, 'r', encoding='utf-8') as f:
            content = f.read()
            if not content.strip():
                raise ValueError(f"Файл {full_path} пустой")
            return content

    def fetch_df(self, path: str) -> pd.DataFrame:
        # Выполнить SQL-запрос и вернуть результат в DataFrame
        sql = self.load_sql(path)
        df = pd.read_sql(sql, self.engine)
        return df

    def fetch_clients_segmentation(self) -> pd.DataFrame:
        return self.fetch_df('sql/client_segmentation.sql')
        logger.info("Данные для загруки в витрину успешно трансформированы")
    
    def load_datamart(self, df: pd.DataFrame, schema: str, table: str) -> None:
        #Загрузить агрегированные данные в витрину
        if df.empty:
            logger.info("DataFrame пуст — загрузка в витрину пропущена.")
            return
    
        sql=self.load_sql('sql/check_duplicate.sql')
        with self.engine.begin() as connection:
            connection.execute(text(sql))
        
        df['last_usage_date'] = pd.to_datetime(df['last_usage_date']).dt.date
           
        # Ожидаемый порядок и набор столбцов
        expected_columns = [
            'client_id', 'client_name', 'client_revenue', 
            'usage_count', 'client_segment', 'last_usage_date'
            ]
        
        #Проверка структуры
        if set(df.columns) != set(expected_columns):
            logger.error("Столбцы в DataFrame не соответствуют ожидаемой структуре таблицы витрины")
            return
        
        df = df[expected_columns]
            
        with self.engine.begin() as connection:
            df.to_sql(
                name=table,
                con=connection,
                schema=schema,
                if_exists='append',
                index=False,
                method='multi'
                )
            
            logger.info("Датафрейм загружен в витрину.")
