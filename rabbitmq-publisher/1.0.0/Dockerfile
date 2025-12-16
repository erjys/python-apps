FROM gcr.io/athena-dashboards-dev-473210/shuffle:app_sdk as base

FROM base as builder
RUN apk --no-cache add --update alpine-sdk libffi libffi-dev musl-dev openssl-dev
RUN mkdir /install
WORKDIR /install
COPY requirements.txt /requirements.txt
RUN pip install --prefix="/install" -r /requirements.txt

FROM base
COPY --from=builder /install /usr/local
# Copy api.yml from current directory
COPY api.yml /app/
# Copy app.py from src subdirectory
COPY src/app.py /app/
WORKDIR /app
CMD ["python", "app.py", "--log-level", "DEBUG"]