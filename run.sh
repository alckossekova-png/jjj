 #!/bin/bash
    set -e

    echo "Остановка и удаление старых контейнеров и томов Docker..."
    docker-compose down -v || true

    echo "Сборка Docker-образов..."
    docker-compose build

    echo "Запуск Docker-контейнеров в фоновом режиме..."
    docker-compose up -d

    echo "Ждем запуск PostgreSQL и ETL-приложения..."
    sleep 45

    echo "ETL-процесс запущен по cron в контейнере etl_app."
    echo "Скрипт extract.py будет запускаться каждые 1 минуту."
    echo "Скрипт transform.py будет запускаться каждые 5 минут."
    echo "Вы можете проверить логи в реальном времени командой: docker-compose logs -f etl_app"
    echo "Или проверить данные в базе данных через psql."