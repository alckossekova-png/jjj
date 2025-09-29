 FROM python:3.10-slim

    RUN apt-get update && apt-get install -y \
        cron \
        postgresql-client \
        build-essential \
        libpq-dev \
        --no-install-recommends && \
        rm -rf /var/lib/apt/lists/*

    WORKDIR /app

    COPY requirements.txt .
    RUN pip install --no-cache-dir -r requirements.txt

    COPY extract.py transform.py table.sql crontab.txt ./

    RUN crontab crontab.txt

    RUN touch /var/log/cron.log

    CMD ["/bin/bash", "-c", "cron -f & tail -f /var/log/cron.log"]