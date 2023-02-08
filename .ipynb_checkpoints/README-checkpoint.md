# Анализ приложения с новостной лентой и мессенджером с курса "Симулятор аналитика"

Описание работ ↓

[A/B тестирование](https://github.com/karinatom/karpov_analyst_simulator/blob/main/AB%20test.ipynb)
- проверка работы системы сплитования
- проверка нового алгоритма рекомендаций в ленте, улучшил ли он показатели CTR

[Построение ETL пайплайна](https://github.com/karinatom/karpov_analyst_simulator/blob/main/ETL_pipeline.py)
- обработка, объединение двух таблиц с информацией о действиях пользователя и запись финальных данных в отдельную таблицу в ClickHouse
- создание дага в Airflow, который будет считаться каждый день за вчера и дополнять таблицу

[Автоматизация отчётности](https://github.com/karinatom/karpov_analyst_simulator/blob/main/daily_report.py)
- создание Telegram бота
- настройка автоматической отправки аналитической сводки по расписанию

[Система алертов](https://github.com/karinatom/karpov_analyst_simulator/blob/main/alert_system.py)
- настройка системы, проверяющей метрики на наличие аномалий каждые 15 минут
- автоматизация системы алертов с помощью Airflow

[Дашборды](https://github.com/karinatom/karpov_analyst_simulator/tree/main/Dashboards)
- система дашбордов для приложения с лентой новостей и мессенджером
- анализ поведения пользователей и эффективности рекламной кампании