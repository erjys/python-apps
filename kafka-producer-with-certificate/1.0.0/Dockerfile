FROM gcr.io/prj-quorum-gc-prod-45bd/shuffle/app_sdk:working as base

FROM base as builder
RUN apk --no-cache add --update alpine-sdk libffi libffi-dev musl-dev openssl-dev

RUN mkdir /install
WORKDIR /install
COPY requirements.txt /requirements.txt
RUN pip install --prefix="/install" -r /requirements.txt

# Final image
FROM base
COPY --from=builder /install /usr/local
COPY api.yml /app/
COPY src/app.py /app/
WORKDIR /app
CMD ["python", "app.py", "--log-level", "DEBUG"]
