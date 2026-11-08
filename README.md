# MLOps 3

## 1. Запуск проекта

Все команды выполняются из корневой директории репозитория.

### Запуск Docker Compose

```bash
docker compose up -d --build
docker compose ps
```

### Первоначальная загрузка данных

Разместите обучающий CSV-файл по пути `data/train.csv`, затем выполните:

```bash
docker compose run --rm ml-cli scripts/load_labeled_csv.py --csv data/train.csv
```

Исходная целевая переменная `target` при загрузке преобразуется в `is_fraud`.

**Важно:** скрипт загрузки очищает ранее загруженную размеченную выборку и состояние обучения. Повторно запускать его следует только при намеренном сбросе данных.

Если после загрузки не инициализировался указатель обучения, его можно восстановить:

```bash
docker compose exec clickhouse clickhouse-client --user analyst --password analyst_password --query "INSERT INTO fraud.training_state (id, next_month) SELECT 1, addMonths(toStartOfMonth(min(transaction_time)), 2) FROM fraud.labeled_transactions"
```

Эта команда предназначена только для первоначальной инициализации при отсутствии записи в `training_state`.

### Подготовка источника данных для dbt

Проект dbt использует представление `transactions_db.transactions`, построенное на основе таблицы `fraud.labeled_transactions` из предыдущего этапа.

Из корня репозитория:

```powershell
Get-Content -Raw -Encoding UTF8 .\sql\06_dbt_source.sql | docker compose exec -T clickhouse clickhouse-client --multiquery
```
**Проверка источника**

```powershell
docker compose exec clickhouse clickhouse-client --query "SELECT count() AS total, sum(target) AS fraud_count, round(avg(target) * 100, 4) AS fraud_rate_pct FROM transactions_db.transactions"
```

```powershell
docker compose exec clickhouse clickhouse-client --query "DESCRIBE TABLE transactions_db.transactions"
```

Представление не дублирует данные: оно читает их из `fraud.labeled_transactions`. При отсутствии исходной таблицы её необходимо предварительно создать и заполнить по инструкции предыдущего этапа.

## 2. Apache Airflow

Веб-интерфейс доступен по адресу:

http://localhost:8081

При первом запуске Airflow в режиме `standalone` создаёт учётную запись администратора.

Пароль можно получить командой:

```bash
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

В проекте реализованы три DAG:

| DAG | Назначение | Расписание |
|---|---|---|
| `fraud_supply_month` | Генерация очередного месяца размеченных данных | Каждые 5 минут, начиная с минуты 0 |
| `fraud_retrain` | Обучение, оценка и публикация модели | Каждые 5 минут, со сдвигом на 2 минуты |
| `fraud_data_quality` | Проверка качества данных и HTML-отчёт | Каждые 15 минут |

Генерация и переобучение разнесены по времени для снижения вероятности одновременного обращения к данным. При этом расписания не образуют строгую меж-DAG-зависимость: если необходимый месяц отсутствует, задача обучения пропускает неполное окно и может обработать его при следующем запуске.

Для первоначальной проверки рекомендуется вручную запустить `fraud_retrain` через **Trigger DAG**, а затем включить автоматические расписания.

Airflow использует `SequentialExecutor` и локальную SQLite-базу.

## 3. Переобучение модели

DAG `fraud_retrain` использует скользящее окно длиной три календарных месяца.

- Первые два месяца используются для обучения.
- Третий месяц используется для тестирования.
- Алгоритм классификации — `CatBoostClassifier`.
- Основная метрика — **Average Precision (AP)**.

### Quality gate

После обучения новая модель сравнивается с текущей опубликованной моделью на одной и той же тестовой выборке.

Новая версия публикуется при выполнении условия:

`AP(candidate) >= AP(production)`

Если кандидат показывает худшее качество, действующая модель сохраняется.

После завершения сравнения указатель обучения переходит к следующему календарному окну, включая случаи отклонения кандидата. Это предотвращает блокировку последующих циклов обучения из-за одного неудачного месяца.

Активная модель определяется файлом `/models/current.json`, а файлы моделей сохраняются в формате `.cbm`.

### Проверка состояния

```bash
docker compose exec fraud-scorer cat /models/current.json
```

```bash
docker compose exec clickhouse clickhouse-client --user analyst --password analyst_password --query "SELECT * FROM fraud.training_state FINAL"
```

Результаты сравнения моделей доступны в XCom соответствующего запуска Airflow.

## 4. Генерация размеченных данных

DAG `fraud_supply_month` создаёт очередной календарный месяц на основе существующих размеченных транзакций.

Генерация выполняется при совпадении указателя обучения с отсутствующим следующим месяцем данных.

Для нового месяца формируется до 10 000 синтетических записей с переносом дат и сохранением исходных признаков и меток.

После успешного добавления выполняется очистка данных за пределами периода хранения в 36 месяцев.

**Ограничение:** синтетические записи формируются из исторических транзакций. Это позволяет продемонстрировать автоматизацию ML-конвейера, но не заменяет независимую тестовую выборку для оценки качества модели в реальной эксплуатации.

Проверка данных по месяцам:

```bash
docker compose exec clickhouse clickhouse-client --user analyst --password analyst_password --query "SELECT toYYYYMM(transaction_time) AS month, count() AS transactions FROM fraud.labeled_transactions GROUP BY month ORDER BY month DESC LIMIT 10"
```

## 5. Онлайн-скоринг транзакций

Сервис `fraud-scorer` получает сообщения из Kafka, формирует признаки и рассчитывает прогноз мошенничества.

Результат включает:

- `score` — оценку вероятности мошенничества;
- `fraud_flag` — бинарное решение модели;
- `model_version` — версию использованной модели.

### Отправка тестовых транзакций

После публикации первой модели:

```bash
docker compose run --rm producer --csv data/test.csv --limit 1000
```

Просмотр логов:

```bash
docker compose logs --tail=100 fraud-scorer
```

Проверка результатов:

```bash
docker compose exec clickhouse clickhouse-client --user analyst --password analyst_password --query "SELECT model_version, count() AS transactions, countIf(fraud_flag = 1) AS detected_fraud, round(avg(score), 4) AS avg_score FROM fraud.scoring_results GROUP BY model_version ORDER BY model_version"
```

Результаты сохраняются в таблице `fraud.scoring_results`.

Сервис проверяет актуальную версию модели при обработке сообщений, что позволяет использовать опубликованную модель без ручного перезапуска контейнера.

## 6. Data Quality

DAG `fraud_data_quality` выполняет шесть проверок:

| Проверка | Описание |
|---|---|
| Nonempty dataset | Набор данных не пуст |
| No null times | Время транзакции заполнено |
| Valid target | Значения целевой переменной допустимы |
| Nonnegative amounts | Суммы транзакций неотрицательны |
| Nonempty category | Категории транзакций заполнены |
| Both target classes | Присутствуют оба класса целевой переменной |

Для каждой проверки формируется статус `PASS` или `FAIL`.

Результат сохраняется в HTML-файле в общем Docker volume `reports`.

Просмотр отчётов:

```bash
docker compose exec airflow ls -lh /reports
```

## 7. Структура проекта

| Файл или директория | Назначение |
|---|---|
| `docker-compose.yml` | Конфигурация сервисов |
| `requirements.txt` | Python-зависимости |
| `scripts/produce_csv.py` | Отправка CSV-транзакций в Kafka |
| `scripts/load_labeled_csv.py` | Загрузка размеченной выборки в ClickHouse |
| `sql/01_schema.sql` | Исходные аналитические таблицы ClickHouse |
| `sql/05_ml_schema.sql` | Таблицы ML-конвейера |
| `mlops/features.py` | Подготовка признаков |
| `mlops/model_store.py` | Хранение и публикация моделей |
| `scoring/consumer.py` | Онлайн-скоринг Kafka-сообщений |
| `dags/retrain_model.py` | Переобучение и quality gate |
| `dags/supply_labeled_month.py` | Генерация синтетических данных |
| `dags/data_quality.py` | Проверки Data Quality и HTML-отчёты |

## 8. Проверенные сценарии

В ходе локального тестирования были проверены:

1. Загрузка 786 431 размеченной транзакции в ClickHouse.
2. Обучение CatBoost на последовательных трёхмесячных окнах.
3. Публикация нескольких версий модели.
4. Отклонение кандидата при ухудшении Average Precision.
5. Отправка и онлайн-скоринг 1000 Kafka-сообщений.
6. Генерация дополнительного календарного месяца.
7. Создание HTML-отчётов Data Quality с шестью успешными проверками.

Автоматическое расписание, устойчивость к повторным запускам и восстановление после частичных сбоев требуют отдельной эксплуатационной проверки.

## 9. Остановка проекта

Остановка контейнеров без удаления данных:

```bash
docker compose stop
```

Повторный запуск:

```bash
docker compose up -d
```

Для сохранения загруженных данных, моделей и состояния Airflow не следует удалять именованные Docker volumes.
