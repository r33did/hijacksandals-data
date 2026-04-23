FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV LANG=C.UTF-8
ENV PATH=/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata \
        openssh-client \
        rsync \
        sshpass \
        libpq-dev \
        postgresql-client \
        gcc \
        g++ \
        build-essential \
        libssl-dev \
        libffi-dev \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_CONSTRAINT_VERSION=3.11

ENV AIRFLOW_HOME=/app/airflow

COPY . /app

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip setuptools==79.0.1 "wheel<0.46" && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir \
        psycopg2 \
        streamlit && \
    pip install --no-cache-dir \
        "apache-airflow[postgres,celery,redis]==${AIRFLOW_VERSION}" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_CONSTRAINT_VERSION}.txt"

EXPOSE 8080 8501