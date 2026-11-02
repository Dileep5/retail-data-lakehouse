# Pipeline image: PySpark + dbt + Great Expectations. Used for the CLI and by CI-style runs.
FROM python:3.12-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
# JAVA_HOME differs per CPU architecture; resolve it at build time.
RUN ln -sfn "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

WORKDIR /opt/lakehouse
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY lakehouse ./lakehouse
COPY analytics ./analytics
COPY dbt ./dbt
COPY sql ./sql

RUN useradd --create-home pipeline && mkdir -p lake data/landing reports && chown -R pipeline /opt/lakehouse
USER pipeline
ENTRYPOINT ["python", "-m", "lakehouse.cli"]
CMD ["--help"]
