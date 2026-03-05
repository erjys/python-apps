ARG BASE_IMAGE=gcr.io/prj-quorum-gc-prod-45bd/shuffle/app_sdk:working
FROM python:3.10-slim AS builder

# Install build dependencies for confluent-kafka (Debian-based, has librdkafka 2.13+)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    libffi-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /install
WORKDIR /install
COPY requirements.txt /requirements.txt
RUN pip install --prefix="/install" -r /requirements.txt

# Final image
FROM python:3.10-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /install /usr/local
COPY src /app
WORKDIR /app
CMD ["python", "app.py", "--log-level", "DEBUG"]
