# DWH продаж и планов - решение тестового задания

# Как запустить

```bash
# 1. Установить зависимости
pip install duckdb

# 2. Запустить ETL процесс
python main.py

# 3. (Опционально) Проверить результат
python validate.py

## Регулярный запуск

Для ежедневного автоматического запуска ETL можно использовать:

### Cron (Linux/macOS)
```bash
# Ежедневно в 8:00
0 8 * * * cd /home/user/project && python main.py >> /var/log/etl.log 2>&1