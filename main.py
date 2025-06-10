# -*- coding: utf-8 -*-

from dotenv import load_dotenv
import os
import pandas as pd

from etl.Database.database import DBExtractor
from etl.Database.DataGeneration import DataGenerator

CSV_DIR = "etl/Database/csv"

def main() -> None:
    #Сгенерировать данные для таблиц (100 записей для клиентов, 200 для продукта)
    generator = DataGenerator()
    generator.generate_clients()
    generator.generate_product_usage()
    
    #Использовать класс dbextractor с методами для реализации etl логики
    extractor = DBExtractor()
    
    #Создать таблицы
    extractor.create_table('clients')
    extractor.create_table('product_usage')
    extractor.create_table('client_segments')
    print("Таблицы успешно созданы")
    
    #Сбросить последовательности
    extractor.seq_restart('staging.clients_id_seq')
    extractor.seq_restart('staging.usage_id_seq')
    print("Последовательности успешно сброшены")
    
    #Загрузить сырые данные на слой staging
    extractor.reading_file(os.path.join(CSV_DIR, "clients.csv"), extractor.loading_clients)
    extractor.reading_file(os.path.join(CSV_DIR, "product_usage.csv"), extractor.loading_usage)
    print("Данные успешно загружены на слой staging")
    
    #Получить аггрегированные данные для загрузки в витрину
    df_client_segments = extractor.fetch_clients_segmentation()
    print("Даные успешно трансформированы")
    
    #Загрузить в витрину
    extractor.load_datamart(df_client_segments, 'data_mart', 'client_segments')
    print("Данные успешно загружены в витрину")
    
    

if __name__ == "__main__":
    main()

