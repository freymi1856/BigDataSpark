# BigDataSpark

Лабораторная работа N2: ETL-пайплайн на Apache Spark.

Решение выполняет обязательную часть задания:

- загружает 10 CSV-файлов `MOCK_DATA*.csv` в PostgreSQL;
- строит модель данных типа "звезда" в PostgreSQL;
- формирует 6 аналитических витрин в ClickHouse.

## Стек

- Docker Compose
- PostgreSQL 16
- Apache Spark 3.5
- ClickHouse 24.12
- PySpark + JDBC-драйверы PostgreSQL и ClickHouse

## Структура проекта

```text
.
├── docker-compose.yml
├── postgres/
│   └── init.sql
├── spark/
│   ├── Dockerfile
│   ├── settings.py
│   ├── io_utils.py
│   ├── load_raw_csv.py
│   ├── build_star_schema.py
│   └── build_clickhouse_reports.py
├── sql/
│   ├── check_postgres.sql
│   └── check_clickhouse.sql
└── исходные данные/
    ├── MOCK_DATA.csv
    ├── MOCK_DATA (1).csv
    └── ...
```

## Запуск

Поднять PostgreSQL, ClickHouse и Spark:

```bash
docker compose up -d --build
```

Загрузить исходные CSV в PostgreSQL:

```bash
docker compose exec spark spark-submit /opt/spark-apps/load_raw_csv.py
```

Построить модель "звезда" в PostgreSQL:

```bash
docker compose exec spark spark-submit /opt/spark-apps/build_star_schema.py
```

Сформировать витрины в ClickHouse:

```bash
docker compose exec spark spark-submit /opt/spark-apps/build_clickhouse_reports.py
```

Остановить окружение:

```bash
docker compose down
```

Полностью удалить данные контейнеров:

```bash
docker compose down -v
```

## Подключения

PostgreSQL:

- host: `localhost`
- port: `15432`
- database: `labdb`
- user: `lab`
- password: `lab`

ClickHouse:

- host: `localhost`
- HTTP port: `18123`
- native port: `19000`
- database: `analytics`
- user: `lab`
- password: `lab`

## Модель PostgreSQL

Сырые данные загружаются в таблицу:

- `raw.mock_data`

Модель "звезда" создается в схеме `dwh`:

- `dwh.fact_sales` - факт продаж;
- `dwh.dim_customer` - покупатели;
- `dwh.dim_seller` - продавцы;
- `dwh.dim_product` - товары;
- `dwh.dim_store` - магазины;
- `dwh.dim_supplier` - поставщики;
- `dwh.dim_date` - календарь продаж.

Так как идентификаторы в CSV повторяются от файла к файлу, ключи измерений создаются как хэш от имени файла и исходного идентификатора. Это защищает модель от случайного объединения разных товаров, клиентов и продавцов.

## Витрины ClickHouse

После выполнения `build_clickhouse_reports.py` создаются 6 таблиц:

- `product_sales_report` - продажи по товарам: выручка, количество продаж, рейтинг, отзывы, топ-10 по количеству;
- `customer_sales_report` - продажи по клиентам: сумма покупок, средний чек, распределение по странам, топ-10 клиентов;
- `time_sales_report` - месячные и годовые тренды: выручка, количество продаж, средний заказ, сравнение с прошлым месяцем;
- `store_sales_report` - продажи по магазинам: выручка, средний чек, распределение по городам и странам, топ-5 магазинов;
- `supplier_sales_report` - продажи по поставщикам: выручка, средняя цена товаров, распределение по странам поставщиков, топ-5;
- `product_quality_report` - качество продукции: рейтинги, отзывы, продажи, корреляция рейтинга и объема продаж.

## Проверка

PostgreSQL можно проверить запросами из файла:

```bash
sql/check_postgres.sql
```

Ожидаемо в `raw.mock_data` должно быть `10000` строк.

ClickHouse можно проверить запросами из файла:

```bash
sql/check_clickhouse.sql
```

Быстрая проверка через контейнер:

```bash
docker compose exec clickhouse clickhouse-client \
  --user lab \
  --password lab \
  --database analytics \
  --query "SELECT count() FROM product_sales_report"
```
