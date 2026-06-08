# Italy Community — Тестовое задание Data Engineer

> **Репозиторий:** https://github.com/TimShar2021/Test_Italy  
> **Стек:** Python · dbt · DuckDB · Apache Airflow · Plotly Dash · Docker

---

## Содержание

1. [Выполнение тестового задания](#1-выполнение-тестового-задания)
   - 1.1 [Структура DWH и DAG](#11-структура-dwh-и-dag)
   - 1.2 [Найденные ошибки в данных и трансформации](#12-найденные-ошибки-в-данных-и-трансформации)
   - 1.3 [Комментарии и допущения](#13-комментарии-и-допущения)
2. [Архитектура проекта](#2-архитектура-проекта)
3. [Быстрый запуск](#3-быстрый-запуск)
4. [Локальный запуск без Airflow](#4-локальный-запуск-без-airflow)
5. [Dashboard](#5-dashboard)
6. [Полезные команды](#6-полезные-команды)

---

## 1. Выполнение тестового задания

### 1.1 Структура DWH и DAG

#### Архитектура хранилища (3-слойный DWH)

Данные поступают из двух источников с принципиально разной природой:
- **POS-система** — транзакционная база с чеками (факт продаж за фев 2026 и фев 2025);
- **Excel-файл** — план продаж, ведётся вручную менеджером.

Выбрана трёхслойная архитектура Staging → DWH → Mart:

```
[POS source DB]      [Excel-план]
       │                   │
       ▼                   ▼
  raw.stg_raw_checks   raw.stg_plan_daily     ← RAW (сырые данные без трансформаций)
  raw.etl_checkpoints
       │                   │
       ▼                   ▼
  staging.stg_checks   staging.stg_plan_daily ← STAGING (очистка, типизация, флаги)
       │
       ▼
  dwh.dim_date         ← DIMENSIONS
  dwh.dim_dish
  dwh.dim_order_type
  dwh.fact_check_items ← FACTS (финальные версии чеков)
  dwh.cancelled_checks ← отдельно: аннулированные и переоткрытые чеки
       │
       ▼
  mart.mart_plan_fact_daily      ← MART (витрины для BI)
  mart.mart_channel_daily
  mart.mart_avg_check_by_order_type
  mart.mart_dish_sales_daily
  mart.mart_dashboard_daily
```

**Ключевые решения по структуре:**

- `raw`-слой хранит данные **без изменений** (включая нулевые версии чеков). Это позволяет при необходимости перегенерировать все слои вниз.
- `staging`-слой отвечает за **нормализацию** строковых значений (каналы, категории), **флаги служебных строк** (МОДИФИКАТОРЫ, УСЛУГИ, ПРОМО) и **детекцию аномалий**.
- `fact_check_items` берёт **только финальную версию** каждого чека: строки с максимальным `close_ts` среди записей с `qty > 0`. Нулевые версии изолируются в `cancelled_checks`.
- **Инкрементальная загрузка** через watermark (`etl_checkpoints`): при каждом запуске читаются только новые строки из источника по `source_row_id > max_loaded`.

#### DAG-дизайн

```
italy_fact_dwh_incremental  (schedule: каждые 15 минут)
├── extract_pos         → extract_pos_incremental.py
│                          ATTACH pos_source.duckdb → читаем новые строки → raw.stg_raw_checks
└── dbt_run_and_test    → dbt run + dbt test
                           staging → dwh → mart (полный перегон витрин)

italy_plan_daily_load   (schedule: 06:00 ежедневно)
└── load_plan           → load_plan.py
                           Читаем лист «план 02 2026» → raw.stg_plan_daily → dbt staging
```

**Почему 15 минут для факта:** POS-данные в реальности обновляются непрерывно, интервал в 15 минут — разумный компромисс между актуальностью и нагрузкой. Для дашборда с оперативным контролем выручки этого достаточно.

**Почему план — отдельный DAG:** план меняется редко (раз в день утром), и его загрузка не должна задерживать основной pipeline с фактом.

---

### 1.2 Найденные ошибки в данных и трансформации

При анализе исходных данных (листы `факт 02 2026` и `факт 02 2025`) выявлено **6 классов ошибок**.

---

#### Ошибка 1: Даты хранятся как серийные числа Excel

**Проблема:** Колонки `Учетный день`, `Время открытия`, `Время закрытия` содержат дробные числа в формате Excel (46054.376... вместо `2026-02-01 09:02:...`).

**Примеры:** `46054.0` → `2026-02-01`, `46054.376655` → `2026-02-01 09:02:22`

**Трансформация (Python — bootstrap_sources.py):**
```python
from datetime import datetime, timedelta

EXCEL_EPOCH = datetime(1899, 12, 30)

def excel_serial_to_dt(serial: float) -> datetime:
    """Конвертация Excel serial date в datetime."""
    return EXCEL_EPOCH + timedelta(days=serial)
```

**SQL (staging — stg_checks.sql):**
```sql
-- Поле уже конвертировано при загрузке в DuckDB как TIMESTAMP
DATE_TRUNC('day', open_ts)  AS open_date,
EXTRACT(hour FROM open_ts)  AS open_hour
```

---

#### Ошибка 2: Многоверсионные (переоткрытые) чеки

**Проблема:** Один номер чека может встречаться с несколькими разными `Временем закрытия`. Строки с `qty = 0` и ранним закрытием — это **аннулированные версии** чека. Строки с `qty > 0` и более поздним закрытием — **финальная версия**.

**Пример:** Чек `39558`, дата `2026-02-01`:
| Время закрытия | Час | Блюдо | qty | Сумма |
|---|---|---|---|---|
| 12:04 | 12 | Д.S.Алла Дьявола | **0** | **0** |
| 14:15 | 14 | Д.S.Алла Дьявола | **0** | **0** |
| 22:22 | 22 | Д.S.Алла Дьявола | **1** | 510.0 |

Версии 12:04 и 14:15 — отменённые состояния. Версия 22:22 — итоговая.

**Трансформация (SQL — fact_check_items.sql):**
```sql
-- Выбираем финальную версию: максимальный close_ts среди строк с qty > 0
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY check_num, accounting_day
               ORDER BY close_ts DESC
           ) AS rn
    FROM staging.stg_checks
    WHERE qty > 0
      AND NOT is_service_item
      AND NOT is_modifier
)
SELECT * FROM ranked WHERE rn = 1
```

**Аннулированные чеки изолируются отдельно:**
```sql
-- cancelled_checks.sql
SELECT DISTINCT check_num, accounting_day, open_ts
FROM staging.stg_checks
WHERE close_ts < (
    SELECT MAX(close_ts)
    FROM staging.stg_checks s2
    WHERE s2.check_num = stg_checks.check_num
      AND s2.accounting_day = stg_checks.accounting_day
)
```

---

#### Ошибка 3: Служебные строки — МОДИФИКАТОРЫ, УСЛУГИ, ПРОМО

**Проблема:** В данных вместе с настоящими блюдами присутствуют строки, которые **не являются позициями меню** и не должны включаться в расчёт выручки и среднего чека по блюдам:
- Категория `МОДИФИКАТОРЫ`: уточнения к блюдам (`обычное`, `Без льда`, `спагетти`, `ветчина (50 г)`, ...). Некоторые имеют ненулевую сумму (доп. ингредиенты), некоторые нулевую.
- Категория `УСЛУГИ`: `Сервисный сбор`, `Доставка 199/299`.
- Категория `ПРОМО`: промо-товары с нулевой суммой (`CHEESE/ S Маргарита`, `PET/вкусняшка + ваучер на груминг`).
- Категория `ДОПЫ`, `АКЦИИ/ПОДАРКИ`, `МАРКИРОВКА`.

**Трансформация (SQL — stg_checks.sql, макрос):**
```sql
-- macros/is_service_item.sql
{% macro is_service_item(category_col) %}
    {{ category_col }} IN (
        'МОДИФИКАТОРЫ', 'УСЛУГИ', 'ПРОМО',
        'ДОПЫ', 'АКЦИИ/ПОДАРКИ', 'МАРКИРОВКА'
    )
{% endmacro %}

-- В stg_checks:
{{ is_service_item('dish_category') }} AS is_service_item,
dish_category = 'МОДИФИКАТОРЫ'        AS is_modifier
```

В `mart_dish_sales_daily` эти строки **отфильтровываются**.  
В `fact_check_items` служебные строки **учитываются в сумме чека** (сервисный сбор — это реальная выручка), но исключаются из топа блюд.

---

#### Ошибка 4: Чеки, открытые до учётного дня

**Проблема:** Чек `96682` имеет `Учетный день = 2026-02-01`, но `Время открытия` соответствует `2026-01-27 08:04` (серийное число 46049). Это реальный кейс для ресторана: стол мог быть открыт несколько дней назад (банкет, ошибка персонала).

**Пример:** `check_num = 96682`, `open_date = 2026-01-27`, `accounting_day = 2026-02-01`, `close_hour = 14`.

**Допущение и трансформация:**  
Такие чеки **включаются в учётный день** (это стандарт для ресторанного учёта — выручка фиксируется по дате закрытия чека). Но в staging добавляется флаг аномалии:

```sql
open_ts::DATE < accounting_day AS is_multiday_check
```

Это позволяет аналитику при необходимости отфильтровать или проанализировать такие чеки отдельно.

---

#### Ошибка 5: Отрицательные суммы — «Округление в пользу гостя»

**Проблема:** Строки с `Блюдо = 'Округление в пользу гостя'` имеют отрицательную сумму (например, `-0.17`, `-0.425`). Это техническая запись при безналичной оплате для выравнивания итога.

**Трансформация:**  
Строки не удаляются, но помечаются:
```sql
dish_name = 'Округление в пользу гостя' AS is_rounding_adjustment
```

При расчёте среднего чека и выручки они **включаются** (корректно уменьшают итог на копейки). В топ блюд не попадают.

---

#### Ошибка 6: Непоследовательная нормализация каналов и категорий

**Проблема:** Названия каналов в поле `Тип заказа` используются по-разному в разных периодах:
- фев 2025: `Доставка курьером`, `Агрегатор (Самовывоз)`, `Самовывоз гостем`
- фев 2026: `Приложение (доставка курьером)`, `Агрегатор (Курьером)`, `Агрегатор (Самовывоз)`

Аналогично категории блюд: в доставочных чеках все блюда имеют префикс `Д.` (`Д.Карбонара`, `Д.ПИЦЦА S`), в ресторанных — без префикса (`«Карбонара»`, `ПАСТА`).

**Трансформация (макрос normalize_channel.sql):**
```sql
{% macro normalize_channel(order_type_col) %}
CASE
    WHEN {{ order_type_col }} ILIKE '%курьер%'         THEN 'delivery_courier'
    WHEN {{ order_type_col }} ILIKE '%агрегатор%курьер%' THEN 'delivery_courier'
    WHEN {{ order_type_col }} ILIKE '%агрегатор%самовывоз%' THEN 'pickup_aggregator'
    WHEN {{ order_type_col }} ILIKE '%самовывоз%'      THEN 'pickup_direct'
    WHEN {{ order_type_col }} ILIKE '%приложение%самовывоз%' THEN 'pickup_app'
    WHEN {{ order_type_col }} ILIKE '%яндекс%'         THEN 'delivery_yandex'
    WHEN {{ order_type_col }} ILIKE '%сайт%'           THEN 'delivery_site'
    WHEN {{ order_type_col }} ILIKE '%ресторан%'       THEN 'dine_in'
    WHEN {{ order_type_col }} ILIKE '%заказ в ресторан%' THEN 'dine_in'
    WHEN {{ order_type_col }} ILIKE '%банкет%'         THEN 'banquet'
    ELSE 'other'
END
{% endmacro %}
```

Нормализация названий блюд — удаление префикса `Д.` и суффиксов ` +`:
```sql
REGEXP_REPLACE(
    REGEXP_REPLACE(dish_name, '^Д\.', ''),
    '\s*\+\s*$', ''
) AS dish_name_normalized
```

---

### 1.3 Комментарии и допущения

**Технологический стек — переход с MS SQL на DuckDB:**  
Задание предполагает работу с MS SQL Server (`dbo.orders`, `dbo.cities` в оригинальном задании про Chibbis). Для данного тестового задания Italy Community исходная POS-система не предоставлена — есть только Excel-выгрузка. Принято решение использовать **DuckDB** в двух файлах: `pos_source.duckdb` имитирует read-only POS-источник, `dwh.duckdb` — аналитическое хранилище. Это позволяет продемонстрировать реальную инкрементальную механику `ATTACH` без внешних зависимостей.

**Определение «финального» чека:**  
Финальной версией чека считается совокупность строк с **максимальным значением `close_ts`** среди строк, где `qty > 0`. Если в рамках одного чека и одного `close_ts` есть несколько блюд — они все финальные. Предположение: POS не может создать два разных финальных состояния с одинаковым временем закрытия.

**Чеки с нулевой суммой:**  
Некоторые блюда (ПРОМО, ДОПЫ-комплименты) имеют нулевую сумму при `qty = 1`. Они **не удаляются** из факта — это реальные операции (подарки, дегустации). При расчёте выручки они автоматически не влияют, но важны для анализа ассортимента.

**Данные февраля 2025:**  
В листе `факт 02 2025` часть строк имеет тип заказа `Доставка курьером` (без уточнения платформы), в то время как в 2026 году появились более детальные значения (`Сайт (доставка курьером)`, `Приложение (доставка курьером)`). Нормализация каналов выравнивает это через `ILIKE`-паттерны, что обеспечивает сопоставимость год к году.

**Средний чек:**  
В витрине `mart_avg_check_by_order_type` средний чек считается как `SUM(amount) / COUNT(DISTINCT check_num)` — по уникальным чекам, а не по строкам. Строки МОДИФИКАТОРЫ и УСЛУГИ **включены** в сумму чека (они реально часть выручки), но исключены из подсчёта позиций меню.

**Plan vs Fact — уровень гранулярности:**  
План предоставлен на уровне **дня** с разбивкой по каналам (ресторан, банкет, три типа доставки). Факт гранулируется до уровня **строки чека**. В витрине `mart_plan_fact_daily` факт агрегируется до дня для сравнения с планом.

---

## 2. Архитектура проекта

```
italy_dwh_project/
├── data/
│   ├── input/italy_test_task.xlsx          # исходный Excel
│   └── runtime/                            # DuckDB-файлы (создаются при запуске)
├── scripts/
│   ├── bootstrap_sources.py                # имитация POS source DB из Excel
│   ├── extract_pos_incremental.py          # incremental extract → raw.stg_raw_checks
│   ├── load_plan.py                        # load Excel plan → raw.stg_plan_daily
│   └── query_sample.py                     # быстрый просмотр mart-таблиц
├── dbt/italy_dwh/
│   ├── models/staging/                     # stg_checks, stg_plan_daily
│   ├── models/dwh/                         # dim_*, fact_check_items, cancelled_checks
│   ├── models/marts/                       # BI-витрины
│   └── macros/                             # normalize_channel, is_service_item, ...
├── dags/
│   ├── italy_fact_dwh_dag.py               # каждые 15 минут
│   └── italy_plan_daily_dag.py             # ежедневно в 06:00
├── dashboard/
│   └── app.py                              # Plotly Dash BI dashboard
├── .dbt/profiles.yml
├── .env.example
├── Dockerfile
├── docker-compose.yml
├── Makefile
└── requirements.txt
```

---

## 3. Быстрый запуск

**Требования:** Docker, Docker Compose.

```bash
git clone https://github.com/TimShar2021/Test_Italy.git
cd Test_Italy

docker compose build
docker compose up -d
```

| Сервис | URL | Логин / Пароль |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Plotly Dashboard | http://localhost:8050 | — |

Включить DAG-и в Airflow:
1. `italy_plan_daily_load` — загрузка плана;
2. `italy_fact_dwh_incremental` — факт POS каждые 15 минут.

---

## 4. Локальный запуск без Airflow

Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 1. Создать имитацию POS-источника из Excel
python scripts/bootstrap_sources.py

# 2. Инкрементальная выгрузка факта в DWH
python scripts/extract_pos_incremental.py

# 3. Загрузить план
python scripts/load_plan.py

# 4. Запустить dbt-трансформации
cd dbt/italy_dwh
dbt run   --profiles-dir ../../.dbt
dbt test  --profiles-dir ../../.dbt
cd ../..

# 5. Проверить результат
python scripts/query_sample.py

# 6. (опционально) Запустить дашборд
python dashboard/app.py       # → http://localhost:8050
```

---

## 5. Dashboard

Plotly Dash dashboard читает данные из `data/runtime/dwh.duckdb`.

**Что отображается:**
- Выбор периода
- KPI-карточки: факт, план, % выполнения, YoY прирост, средний чек, выручка на гостя
- График план/факт по дням
- Сравнение с прошлым годом (аналогичные даты фев 2025)
- Выручка по каналам
- Динамика среднего чека
- Топ блюд по выручке
- Таблица детализации по дням

**Витрины на бэкенде:**
- `mart.mart_dashboard_daily`
- `mart.mart_channel_daily`
- `mart.mart_dish_sales_daily`

---

## 6. Полезные команды

```bash
make build               # сборка Docker image
make up                  # поднять Airflow
make down                # остановить
make clean               # удалить runtime DuckDB и logs

make local-bootstrap     # bootstrap_sources.py
make local-extract       # extract_pos_incremental.py
make local-plan          # load_plan.py
make local-dbt-run       # dbt run
make local-dbt-test      # dbt test
make local-query         # query_sample.py
make local-dashboard     # запустить дашборд на http://localhost:8050
make dashboard-logs      # логи dashboard-контейнера
```
