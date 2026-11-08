# GoFuture — переход от монолита к глобальной микросервисной платформе

Архитектурный проект эволюции платформы агрегации такси «GoFuture» из Django-монолита в
мультирегиональную, событийно-ориентированную и мультитенантную микросервисную платформу.
Все решения оформлены как ADR, диаграммы подготовлены в нотации C4.

## Результаты по этапам

### 1. План декомпозиции монолита

Поэтапный план перехода на доменные сервисы с минимизацией простоя.

| ADR | Тема | Артефакт |
|-----|------|----------|
| ADR-001 | Нефункциональные требования (НФТ) | [ADR-001-nfr.md](Task1/adr/ADR-001-nfr.md) |
| ADR-002 | Карта доменных сервисов | [ADR-002-service-map-and-c2.md](Task1/adr/ADR-002-service-map-and-c2.md) |
| ADR-003 | Очерёдность выделения сервисов | [ADR-003-extraction-order.md](Task1/adr/ADR-003-extraction-order.md) |
| ADR-004 | Обратная совместимость | [ADR-004-backward-compatibility.md](Task1/adr/ADR-004-backward-compatibility.md) |
| ADR-005 | План миграции данных | [ADR-005-data-migration-plan.md](Task1/adr/ADR-005-data-migration-plan.md) |
| ADR-006 | Kafka как event-driven backbone | [ADR-006-kafka-event-backbone.md](Task1/adr/ADR-006-kafka-event-backbone.md) |

| Диаграмма                              | PlantUML | PNG |
|----------------------------------------|----------|-----|
| Диаграмма контейнеров | [c2-to-be.puml](Task1/diagrams/c2-to-be.puml) | [c2-to-be.png](Task1/diagrams/c2-to-be.png) |

### 2. Событийная платформа

Модель доменных событий, топики Kafka с партиционированием по регионам, потоковая обработка,
Saga, надёжная доставка и мониторинг.

| ADR | Тема                                         | Артефакт |
|-----|----------------------------------------------|----------|
| ADR-007 | Модель доменных событий                      | [ADR-007-domain-events.md](Task2/adr/ADR-007-domain-events.md) |
| ADR-008 | Топики Kafka и партиционирование по регионам | [ADR-008-kafka-topics-and-partitioning.md](Task2/adr/ADR-008-kafka-topics-and-partitioning.md) |
| ADR-009 | Обработка событий                            | [ADR-009-stream-processing.md](Task2/adr/ADR-009-stream-processing.md) |
| ADR-010 | Вид Saga                                     | [ADR-010-saga.md](Task2/adr/ADR-010-saga.md) |
| ADR-011 | Механизмы надёжной доставки событий          | [ADR-011-reliable-delivery.md](Task2/adr/ADR-011-reliable-delivery.md) |
| ADR-012 | Мониторинг событийной платформы              | [ADR-012-monitoring.md](Task2/adr/ADR-012-monitoring.md) |

| Диаграмма                              | PlantUML | PNG |
|----------------------------------------|----------|-----|
| Диаграмма контейнеров | [c2-to-be.puml](Task2/diagrams/c2-to-be.puml) | [c2-to-be.png](Task2/diagrams/c2-to-be.png) |

### 3. Глобальное развёртывание и отказоустойчивость

Выбор регионов, репликация данных, геомаршрутизация, failover и комплаенс,
а также средства защиты в ландшафте (WAF/Firewall).

| ADR | Тема | Артефакт |
|-----|------|----------|
| ADR-013 | Выбор регионов глобального развёртывания | [ADR-013-deployment-regions.md](Task3/adr/ADR-013-deployment-regions.md) |
| ADR-014 | Схема репликации данных между регионами | [ADR-014-data-replication.md](Task3/adr/ADR-014-data-replication.md) |
| ADR-015 | Геомаршрутизация пользователей | [ADR-015-geo-routing.md](Task3/adr/ADR-015-geo-routing.md) |
| ADR-016 | Failover, точки отказа и сценарии | [ADR-016-failover.md](Task3/adr/ADR-016-failover.md) |
| ADR-017 | Стратегия комплаенса | [ADR-017-compliance.md](Task3/adr/ADR-017-compliance.md) |
| ADR-018 | Средства защиты ландшафта (WAF, Firewall, IDS/IPS, mTLS) | [ADR-018-security-landscape.md](Task3/adr/ADR-018-security-landscape.md) |

| Диаграмма | PlantUML | PNG |
|-----------|----------|-----|
| Диаграмма контейнеров | [c2-to-be.puml](Task3/diagrams/c2-to-be.puml) | [c2-to-be.png](Task3/diagrams/c2-to-be.png) |
| Репликация данных | [replication.puml](Task3/diagrams/replication.puml) | [replication.png](Task3/diagrams/replication.png) |
| Геомаршрутизация | [georouting.puml](Task3/diagrams/georouting.puml) | [georouting.png](Task3/diagrams/georouting.png) |
| Failover | [failover.puml](Task3/diagrams/failover.puml) | [failover.png](Task3/diagrams/failover.png) |

### 4. Платформа данных и ML

Lakehouse-платформа: сбор данных из микросервисов, ML-платформа (features/обучение/инференс),
BI-слой, качество данных и резидентность.

| ADR | Тема | Артефакт |
|-----|------|----------|
| ADR-019 | Платформа данных и модель хранения | [ADR-019-data-platform-lakehouse.md](Task4/adr/ADR-019-data-platform-lakehouse.md) |
| ADR-020 | Сбор данных из микросервисов | [ADR-020-data-ingestion.md](Task4/adr/ADR-020-data-ingestion.md) |
| ADR-021 | ML-платформа | [ADR-021-ml-platform.md](Task4/adr/ADR-021-ml-platform.md) |
| ADR-022 | Аналитика и BI-слой | [ADR-022-analytics-bi.md](Task4/adr/ADR-022-analytics-bi.md) |
| ADR-023 | Качество данных и data governance | [ADR-023-data-quality-governance.md](Task4/adr/ADR-023-data-quality-governance.md) |
| ADR-024 | Резидентность и мультирегиональность | [ADR-024-data-residency.md](Task4/adr/ADR-024-data-residency.md) |

| Диаграмма             | PlantUML | PNG |
|-----------------------|----------|-----|
| Диаграмма контейнеров | [c2-to-be.puml](Task4/diagrams/c2-to-be.puml) | [C2-to-be.png](Task4/diagrams/C2-to-be.png) |

### 5. Мультитенантная платформа и IAM

Изоляция данных партнёров, IAM на Keycloak с RBAC/ABAC и SSO, автоматизированный онбординг,
мониторинг мультитенантного окружения.

| ADR | Тема | Артефакт |
|-----|------|----------|
| ADR-025 | Модель изоляции данных между партнёрами | [ADR-025-tenant-isolation.md](Task5/adr/ADR-025-tenant-isolation.md) |
| ADR-026 | IAM (Keycloak), SSO/OIDC и ролевой доступ (RBAC+ABAC) | [ADR-026-iam-keycloak-rbac.md](Task5/adr/ADR-026-iam-keycloak-rbac.md) |
| ADR-027 | Автоматизированный онбординг партнёров | [ADR-027-partner-onboarding.md](Task5/adr/ADR-027-partner-onboarding.md) |
| ADR-028 | Мониторинг мультитенантного окружения | [ADR-028-multitenant-monitoring.md](Task5/adr/ADR-028-multitenant-monitoring.md) |

| Диаграмма                                                  | PlantUML | PNG |
|------------------------------------------------------------|----------|-----|
| Диаграмма контейнеров                                      | [c2-to-be.puml](Task5/diagrams/c2-to-be.puml) | [c2-to-be.png](Task5/diagrams/c2-to-be.png) |
| Диаграмма последовательности процесса онбординга партнеров | [partner-onboarding.puml](Task5/diagrams/partner-onboarding.puml) | [partner-onboarding.png](Task5/diagrams/partner-onboarding.png) |

Дополнительные артефакты: [Матрица ролей и доступа](Task5/roles-and-access-matrix.md)

## Как читать артефакты

Решение каждой задачи содержит каталоги `adr/` и `diagrams/`. В каталоге `adr/`
каждый ADR содержит контекст, требования, решение, рассмотренные альтернативы и компромиссы.
Диаграммы в `diagrams/` представлены исходником PlantUML (`.puml`) и готовым рендером (`.png`).
