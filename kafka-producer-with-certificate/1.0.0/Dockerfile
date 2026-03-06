FROM gcr.io/prj-quorum-gc-prod-45bd/shuffle/app_sdk:working AS base

FROM base AS builder
RUN apk --no-cache add --update \
    alpine-sdk \
    librdkafka-dev \
    libffi-dev \
    openssl-dev

WORKDIR /install
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --prefix="/install" -r requirements.txt

FROM base
RUN apk --no-cache add --update librdkafka
COPY --from=builder /install /usr/local
COPY api.yml src/ /app/
WORKDIR /app
CMD ["python", "app.py", "--log-level", "DEBUG"]