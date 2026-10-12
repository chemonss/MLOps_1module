# MLOps — Kafka + ClickHouse

Проект реализует загрузку транзакций из CSV-файла в Kafka с последующим сохранением в ClickHouse. Реализованы SQL-запросы для определения категории самой крупной транзакции в каждом штате США и оптимизация хранения данных.

## 1. Требования

- Docker и Docker Compose
- Git
- CSV-датасет с [Kaggle](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394)

## 2. Запуск проекта

**1. Клонировать репозиторий:**

```bash
git clone https://github.com/chemonss/MLOps_1module.git
cd MLOps_1module
```

**2. Подготовить данные:**

Скачать датасет с Kaggle и поместить CSV-файл в директорию `data/` под именем `test.csv`.

**3. Запустить Kafka и ClickHouse:**

```bash
docker compose up -d --build
```

**4. Загрузить CSV в Kafka:**

```bash
docker compose run --rm producer --csv data/test.csv
```

Для тестирования на первых 10 000 строках:

```bash
docker compose run --rm producer --csv data/test.csv --limit 10000
```

## 3. Проверка загрузки

Проверить количество загруженных транзакций:

```bash
docker compose exec clickhouse clickhouse-client \
  --user analyst --password analyst_password \
  --query "SELECT count() FROM fraud.transactions"
```

После отправки данных необходимо дождаться их обработки ClickHouse.

## 4. Выполнение SQL-запросов

В проекте находятся следующие SQL-файлы:

- `sql/01_schema.sql` — создание таблиц и materialized views.
- `sql/02_query_baseline.sql` — поиск категории максимальной транзакции по каждому штату.
- `sql/03_query_optimized.sql` — оптимизированный запрос через предварительную агрегацию.
- `sql/04_schema_baseline.sql` — создание неоптимизированной таблицы для сравнения.

Для выполнения запросов и экспорта результатов в CSV (Linux, macOS или Git Bash):

```bash
bash scripts/export_results.sh
```

Результаты сохраняются в:

- `results/baseline.csv`
- `results/optimized.csv`

## 5. Остановка проекта

Остановить контейнеры:

```bash
docker compose down
```

Полностью удалить контейнеры и сохранённые данные:

```bash
docker compose down -v
```

**Примечание:** команда `down -v` удаляет тома Kafka и ClickHouse. При следующем запуске потребуется повторная загрузка данных.
