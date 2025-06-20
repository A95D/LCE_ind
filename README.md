# ETL процесс для сегментации клиентов по использованию банковских продуктов

## Описание проекта

Этот проект демонстрирует процесс построения аналитической витрины клиентов банка — от генерации синтетических данных до загрузки и визуализации в Power BI.  
В процессе ETL данные проходят через слои: `staging` → `data_mart` → Power BI.

База данных состоит из слоев:
- `staging`
- `data_mart`

В результате строится витрина, содержащая сегментированных клиентов на основе их активности.  
Запуск системы осуществляется в командной строке.

---

## Используемый стек

### Бэкенд
- Python 3.12+
- PostgreSQL

### Библиотеки
- SQLAlchemy
- pandas
- python-dotenv
- logging

### Инструменты
- Power BI (для визуализации)

---

## Пример работы отчета в Power BI

![power_bi_example](sources/gifs/demonstration.gif)

---

## Исходные данные

Исходные данные передаются в `.jsonl` файлах:
- `names.jsonl` — данные по именам  
- `midnames.jsonl` — данные по отчествам  
- `surnames.jsonl` — данные по фамилиям

---

## Установка и запуск

### 1. Клонировать проект:
```bash
git clone <метод копирования>
```

Будут скопированы 3 рабочих файла:
- `main.py` — основной скрипт запуска ETL
- `database.py` — работа с БД, содержит класс `DBExtractor` (создание таблиц, загрузка, извлечение данных)
- `DataGeneration.py` — генерация тестовых данных

### 2. Установить зависимости:
```bash
pip install -r requirements.txt
```

### 3. Создать файл `.env`:
```bash
touch .env
```

В `.env` прописать параметры подключения и пути к файлам:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=league
DB_USER=myuser
DB_PASS=...

PATH_NAMES=etl/Database/jsonl/names_table.jsonl
PATH_MIDNAMES=etl/Database/jsonl/midnames_table.jsonl
PATH_SURNAMES=etl/Database/jsonl/surnames_table.jsonl
PATH_CLIENTS=etl/Database/csv/Инд. проект/clients.csv
PATH_USAGE=etl/Database/csv/product_usage.csv
```

### 4. Запустить систему:
```bash
python main.py
```

---

## Компоненты ETL-процесса

### Генерация данных

`DataGeneration.py` — модуль для создания синтетических данных клиентов банка и их продуктов.  
Данные генерируются с использованием имен с частотой выше 5000.

#### 1. Генерация клиентов:
- Реалистичные ФИО с учетом гендера  
- Случайная величина дохода (30 000 – 500 000 руб.)  
- Сохранение в CSV (`clients.csv`)

#### 2. Генерация использования продуктов:
- 11 случайных финансовых продуктов (кредиты, вклады, карты, страхование)  
- Случайные даты операций (май 2025)  
- Сохранение в CSV (`product_usage.csv`)

#### Использование:
```python
generator = DataGenerator()
generator.generate_clients()
generator.generate_product_usage()
```

---

### ETL-ядро

`database.py` — модуль для работы с базой данных PostgreSQL, реализующий ETL-процесс.  
Происходит загрузка данных из CSV на слой `staging`, агрегация и загрузка в `data_mart`.

#### 1. Инициализация:
Создает соединение с PostgreSQL, используя `.env`.

#### 2. Основные методы:
- `seq_restart()` — сбрасывает sequence ID перед новой загрузкой  
- `reading_file()` — базовое чтение CSV  
- `loading_clients()` — загрузка клиентов в `staging.clients`  
- `loading_usage()` — загрузка продуктов в `staging.product_usage`  
- `load_datamart()` — загрузка агрегатов в `data_mart.client_segments`

#### 3. Работа со структурой БД:
- `create_table()` — создает таблицы, если не существуют: `staging.clients`, `staging.product_usage`, `data_mart.client_segments`

#### 4. SQL-операции:
- `load_sql()` — загрузка SQL из файла  
- `fetch_df()` — выполнение SQL и возврат `DataFrame`  
- `fetch_clients_segmentation()` — получение сегментированных клиентов

#### 5. Пример использования:
```python
extractor = DBExtractor()

# Создание таблиц
extractor.create_table('clients')
extractor.create_table('product_usage')
extractor.create_table('client_segments')

# Сброс последовательностей
extractor.seq_restart('staging.clients_id_seq')
extractor.seq_restart('staging.usage_id_seq')

# Загрузка данных
extractor.reading_file(os.path.join(CSV_DIR, "clients.csv"), extractor.loading_clients)
extractor.reading_file(os.path.join(CSV_DIR, "product_usage.csv"), extractor.loading_usage)

# Получение и загрузка агрегатов
df_client_segments = extractor.fetch_clients_segmentation()
extractor.load_datamart(df_client_segments, 'data_mart', 'client_segments')
```

---

## Слой с аналитической витриной

Этот слой — результат ETL, откуда Power BI получает данные.

Агрегируются данные по каждому клиенту и присваивается сегмент на основе количества использованных продуктов.

### Структура витрины:

| Поле                     | Тип данных | Описание                                          |
|--------------------------|------------|--------------------------------------------------|
| `client_id`              | INTEGER    | Идентификатор клиента                             |
| `client_name`            | VARCHAR    | Полное имя клиента                                |
| `client_revenue`         | INTEGER    | Доход клиента, ₽                                  |
| `usage_count`            | INTEGER    | Кол-во использованных продуктов                   |
| `client_segment`         | VARCHAR    | Название сегмента                                 |
| `last_product_usage_date`| DATE       | Дата последнего использования продукта            |

### Примеры сегментов:
- 1 продукт  
- 2 продукта  
- 3+ продуктов

### Пример записи:
```csv
client_id,client_name,client_revenue,usage_count,client_segment,last_product_usage_date
83,Азамат Филиппович Румянцев,121000,1,1 продукт,2025-05-24
```

---

## Демонстрация отчета

### Источник данных
В Power BI указать источник: **База данных PostgreSQL**

### Модель данных

Используются таблицы:
- `data_mart.client_segments` — основная
- `staging.product_usage` — для детализации
- `usage_freq` — вспомогательная таблица Power Query

Модель:
![data_model_example](sources/pictures/data_model.png)

---

## Пример анализа

Одна из целей отчета — предложить кампанию для клиентов с 1 продуктом.

### 1. Выявление клиентов с 1 продуктом:
На круговой диаграмме выбрать сегмент "1 продукт" (синим цветом):  
![analysis_example1](sources/pictures/example1.png)

### 2. Анализ популярных продуктов:
По линейчатой диаграмме — топ-3:
- Кредитная карта (5 клиентов)
- Вклад 6 мес. (3 клиента)
- Дебетовая карта (2 клиента)

При равной частоте — сортировать по убыванию "Общего количества".

### 3. Пример персонального предложения:
Клиент: **Роберт Павлович Зорин**  
Он пользовался только "Страхование жизни".  
Предложение: "Кредитная карта" — популярный продукт в его сегменте.  
![analysis_example2](sources/pictures/example2.png)

---

## Цели проекта

Проект создан в учебных целях — это практическое задание в рамках стажировки.
