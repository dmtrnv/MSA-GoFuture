# ADR-012: Мониторинг событийной платформы

- **Автор:** Дмитрий Ренев
- **Статус:** На согласовании
- **Согласующие:** Дмитрий Бардин
- **Дата:** 2026-10-14
- **Связанные артефакты:** [ADR-001-nfr.md](../../Task1/adr/ADR-001-nfr.md), [ADR-008-kafka-topics-and-partitioning.md](ADR-008-kafka-topics-and-partitioning.md), [ADR-009-stream-processing.md](ADR-009-stream-processing.md), [ADR-011-reliable-delivery.md](ADR-011-reliable-delivery.md), [c2-to-be.puml](../diagrams/c2-to-be.puml)

---

## Контекст

Необходимо выбрать подходы к мониторингу, составить список метрик, определить и интегрировать инструменты мониторинга в архитектуру. 

Событийная платформа добавляет новый класс отказов, который не виден в обычных метриках сервисов:

- **consumer lag** (потребитель отстаёт);
- **outbox publish lag** (данные записаны, событие ещё не опубликовано - ADR-011);
- **рост DLQ/retry** (сбои обработки);
- **ребалансировка consumer group** (потеря прогресса/дубликаты);
- **здоровье Flink-job'ов** (checkpoint, backpressure, watermark lag) - ADR-009;
- **publish-latency** end-to-end (от `occurredAt` до обработки).


## Требования

1. 100 % покрытие критичных сервисов и потоков метриками и алертами (NFR-O-01).
2. Сквозная трассировка по `correlationId` (NFR-O-02) - включая путь события.
3. Централизованные логи (NFR-O-03).
4. Контроль consumer lag, throughput брокера (NFR-O-04), DLQ, outbox.
5. Привязка к SLO/error budget (доступность 99,95 %, NFR-A-01) с алертингом по выгоранию бюджета, а не по каждому отклонению.
6. Наблюдаемость потоковых job'ов (Flink).

## Решение

Существующий стек (Prometheus, Grafana, Loki, Alertmanager) сохраняется, но расширяется.

### Подход: от "алертов на метрики" к SLO-driven observability

- **Три столпа:** метрики (Prometheus), логи (Loki), распределённые трейсы (**Jaeger** - новый компонент).
- **SLI → SLO → error budget:** для каждого критичного сценария задаётся SLI (например, доля событий, обработанных < 5 c), SLO (≥ 99,95 %) и бюджет ошибок. Алерты - на **скорость выгорания** бюджета (multi-window, multi-burn-rate), а не на единичные всплески. Это снижает alert fatigue при 500k поездок.
- **Golden signals** для каждого сервиса (latency, traffic, errors, saturation) + **event-specific signals** (lag, DLQ, outbox, rebalance).
- **Сквозной контур:** OpenTelemetry Collector - единая точка приёма телеметрии (метрики/трейсы/логи) от сервисов.

### Топология сбора метрик - инфраструктура напрямую, OTel для приложений

Prometheus по природе **pull-овый**. Метрики собираются двумя путями:

- **Инфраструктурные метрики - Prometheus скрейпит напрямую** (штатный режим экспортеров, минимум точек отказа и промежуточных узлов):
  - **Postgres Exporter** (`pg_*`), **Redis Exporter** (`redis_*`), **Elasticsearch Exporter** (`elasticsearch_*`), **Kafka Metrics Exporter** (lag/throughput).
- **Прикладная телеметрия и ClickHouse - через OpenTelemetry Collector**:
  - принимает метрики/трейсы/логи сервисов и Flink по OTLP;
  - скрейпит **ClickHouse** (`/metrics`, нативный Prometheus-эндпоинт - отдельный экспортер не нужен);
  - обогащает лейблами (регион, сервис, `correlationId`) и батчит;
  - отдаёт метрики в Prometheus, трейсы - в Jaeger, логи - в Loki.

Итог: Prometheus собирает из двух источников - прямые инфра-экспортеры и OTel Collector (приложения + ClickHouse).

### Инструменты и их роль

| Инструмент                            | Роль | Что добавилось |
|---------------------------------------|------|-----------------------|
| **Prometheus**                        | Хранилище метрик (pull); скрейпит инфра-экспортеры напрямую и OTel Collector | Правила записи SLI/SLO |
| **OpenTelemetry Collector** *(новый)* | Прикладная телеметрия: приём OTLP (сервисы, Flink), скрейп ClickHouse `/metrics`, обогащение лейблами/`correlationId`; `prometheusexporter`→Prometheus, трейсы→Jaeger, логи→Loki | Основной сборщик прикладной телеметрии |
| **Kafka Metrics Exporter** *(новый)*  | Метрики consumer lag, offset, throughput, размер партиций | Ключевой источник для SLO по обработке событий |
| **Postgres Exporter** *(новый)*       | Метрики всех БД PostgreSQL (по инстансу на каждую БД сервиса) | `pg_*`: connections, replication lag, slow queries, размер БД, рост outbox |
| **Redis Exporter** *(новый)*          | Метрики кеша Redis (`INFO`, `COMMANDSTATS`) | `redis_*`: memory/fragmentation, evictions, hit/miss, clients, replication |
| **Elasticsearch Exporter** *(новый)*  | Метрики геоиндекса Elasticsearch (Stats API) | `elasticsearch_*`: cluster health, JVM, thread pools, shards, latency |
| **JMX Exporter** *(новый)*            | JMX-метрики JVM Schema Registry | `schema_registry_*`: запросы/ошибки, master/slave, число схем, отказы совместимости |
| **ClickHouse**                        | `/metrics` (Prometheus-формат) + system-таблицы | Скрейпится OTel напрямую, отдельный экспортер не нужен |
| **Grafana**                           | Единое окно observability: дашборды (SLO/error budget, lag, DLQ, outbox, Flink, хранилища), запросы к Prometheus (PromQL), Loki (LogQL), Jaeger (трейсы) и Alertmanager (алерты/силенсы) | Дашборды событийной платформы и сквозная диагностика; запросы к **Jaeger** - впервые |
| **Loki**                              | Централизованные логи | Логи продюсеров/консьюмеров/DLQ |
| **Jaeger** *(новый)*                  | Распределённые трейсы | Трейс события от продюсера до потребителя и саги |
| **Alertmanager**                      | Маршрутизация алертов | Мультиоконные burn-rate алерты, алерты на lag/DLQ/Flink |

### Список метрик

**A. Брокер и потоки (event-specific, критические)**

| Метрика | Описание | Целевой порог / алерт |
|---------|----------|------------------------|
| `kafka_consumergroup_lag` | Лаг по consumer group / партиции | warn > 5k, crit > 20k сообщений |
| `kafka_topic_partition_throughput` | msg/s и bytes/s по топику | деградация ниже ёмкости |
| `kafka_consumer_rebalance_total` | Частота ребалансировок | > 1 / 5 мин - алерт |
| `event_end_to_end_latency` (p95/p99) | `processedAt − occurredAt` | p95 ≤ 5 c (NFR-связка) |
| `outbox_publish_lag` | Задержка relay публикации из outbox | p95 ≤ 2 c |
| `outbox_backlog_size` | Неопубликованных записей | рост - алерт |
| `dlq_message_total` / `dlq_size` | Попадания и размер DLQ | любой рост - алерт |
| `retry_topic_rate` | Частота ретраев | всплеск - алерт |
| `schema_registry_incompatible_total` | Отклонённые несовместимые схемы | > 0 - алерт |
| `schema_registry_*` (JMX Exporter) | JMX-метрики Schema Registry: запросы/ошибки, master/slave, число схем | `service_up` = 0 или рост ошибок - алерт |
| `produce_error_rate` | Ошибки продюсера | > 0 - алерт |

**B. Потоковые job'ы (Flink)**

| Метрика | Описание | Порог |
|---------|----------|-------|
| `flink_job_restarts_total` | Рестарты job'а | любой - алерт |
| `flink_checkpoint_duration` / `_failed` | Чекпоинты | failed > 0 - алерт |
| `flink_backpressure_level` | Backpressure операторов | > 0.5 - warn |
| `flink_watermark_lag` | Отставание watermark | > допустимого окна |
| `flink_numRecordsInPerSecond` | Пропускная способность джоба | деградация |

**C. Сервисы (golden signals)**

| Метрика | Порог |
|---------|-------|
| `gateway_request_rate` / `gateway_request_duration` (p95/p99) | p95 ≤ 300 мс, p99 ≤ 800 мс (NFR-P-02); RPS/RPM через шлюз |
| `gateway_5xx_rate` | 5xx rate - по error budget |
| `gateway_upstream_errors_total` | ошибки/таймауты вызовов сервисов - алерт |
| `gateway_rate_limited_total` | всплеск отказов rate limiting - warn |
| `gateway_auth_failures_total` | всплеск 401/403 - warn |
| `http_request_duration` (p95/p99) | p95 ≤ 300 мс, p99 ≤ 800 мс (NFR-P-02) |
| `http_requests_total{status}` | 5xx rate - по error budget |
| `matching_duration` (p95) | ≤ 1 c (NFR-P-03) |
| `pricing_calc_duration` (p95) | ≤ 200 мс (NFR-P-04) |
| `saga_state_duration` / `saga_stuck_total` | зависшие саги > таймаута |
| `notification_delivery_total{channel,status}` / `_latency` | доля успешных отправок (FCM/APNS/Huawei), ошибки/задержка - warn |
| `fraud_scoring_duration` (p95) | ≤ 200 мс (критичный путь платежа) |
| `fraud_decisions_total{decision}` | аномалия доли фрод-срабатываний |
| `analytics_ingest_rate` / `analytics_consumer_lag` | отставание ingest в ClickHouse |
| `service_up` / readiness | падение - алерт |

**D. Инфраструктура и БД**

| Метрика | Порог |
|---------|-------|
| `pg_*` (Postgres Exporter, все БД) | connections, replication lag, slow queries, размер БД, рост outbox |
| `redis_*` (Redis Exporter) | used_memory и fragmentation > 80 %, `evicted_keys` > 0 → алерт, hit/miss ratio, connected clients, replication lag, `LATENCY`/slowlog |
| `clickhouse_*` (нативный `/metrics`) | query duration/throughput, `parts` и `merges` на партицию, replication queue (`ReplicatedMergeTree`), disk/memory, failed inserts, mutations |
| `elasticsearch_*` (Elasticsearch Exporter) | cluster health ≠ green → алерт, JVM heap > 85 %, thread pool rejects (search/write), search/index latency, unassigned shards > 0, disk watermark |
| `container_cpu/mem` | saturation против лимитов |

**E. Бизнес-SLI (для SLO)**

| SLI | SLO |
|-----|-----|
| Доля успешных создания поездки (HTTP 2xx / все) | ≥ 99,95 % |
| Доля событий, обработанных < 5 c | ≥ 99,9 % |
| Доля платежей без ошибки/pending > 60 c | ≥ 99,95 % |

### Дашборды и алерты

- **Дашборды Grafana:** "Kafka health" (lag/throughput/rebalance), "Event flow" (e2e latency, outbox, DLQ), "Flink jobs", "Saga overview", "API Gateway" (RPS, latency, 5xx, rate-limit/auth), "SLO & error budget", "External deps" (Яндекс Пэй/Карты/Банк).
- **Правила алертов (Alertmanager):** multi-window burn-rate по SLO + пороги из таблиц A–C; маршрутизация по доменам к командам и SRE; критичные - в чат.
- **Runbook** на каждый критичный алерт (lag, DLQ, breakpoint Flink, outbox backlog).

## Альтернативы

1. **Только метрики брокера (без lag-экспортера).** Не видно отставания потребителей - критичный пробел.
2. **Алерты на каждую метрику (без SLO).** Alert fatigue и пропуск реальных инцидентов на 500k нагрузке.
3. **Полностью managed observability (Datadog/New Relic).** Меньше операционной нагрузки, но дороже и дублирует существующий стек (Prometheus/Grafana).
4. **Нативные ресиверы OTel** (`redisreceiver`, `postgresqlreceiver`, `elasticsearchreceiver`) вместо экспортеров. Меньше контейнеров, но меньше метрик и зрелости, чем у специализированных экспортеров.

## Компромиссы и риски

- **OpenTelemetry Collector критичен для прикладной телеметрии и ClickHouse**: при его отказе Prometheus не получает метрики приложений и ClickHouse, Jaeger - трейсы, Loki - логи (инфраструктурные метрики продолжают поступать напрямую). Митигация: HA (несколько реплик), буферизация/очереди (`sending_queue`, `retry`), backpressure, отдельные реплики под разные сигналы (metrics/traces/logs).
- **Нагрузка на OTel Collector** складывается из OTLP-потока приложений и скрейпа ClickHouse - требует ёмкостного планирования.
- **Дополнительный хоп** (приложение/ClickHouse → OTel → Prometheus) повышает задержку доставки таких метрик - некритично для алертинга, но учитывать при тонких SLO. Инфраструктурные метрики идут напрямую, без этого хопа.
- **Два пути сбора** (прямой скрейп + OTel) - нужно следить за единообразием лейблов (`region`, `service`) между источниками, чтобы SLI считались корректно.
- **Высокая кардинальность метрик** (регион × топик × партиция × consumer group) → рост стоимости Prometheus. Митигация: агрегация, лимиты, запись SLI на стороне Prometheus.
- **Burn-rate алерты** сложнее в настройке; требуется обкатка порогов.
- **Flink-метрики** требуют reporter-конфигурации и дашбордов - дополнительная работа.
- **Трейсинг событий** требует дисциплины проброса `correlationId` через консьюмеров и outbox; без него end-to-end трейс рвётся.

## Допущения

- Существующие Prometheus/Grafana/Loki/Alertmanager масштабируются под возросший объём телеметрии (или берутся managed-версии).
- Redis Exporter/Elasticsearch Exporter совместимы с версиями Redis и Elasticsearch в ландшафте; ClickHouse отдаёт `/metrics` в Prometheus-формате.
- OTel Collector разворачивается в HA и имеет доступ к endpoint'ам экспортеров и ClickHouse.
