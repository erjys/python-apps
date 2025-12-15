# Base our app image off of the WALKOFF App SDK image
FROM frikky/shuffle:app_sdk as base

# Builder stage to pre-install dependencies
FROM base as builder
RUN apk --no-cache add --update alpine-sdk libffi libffi-dev musl-dev openssl-dev
RUN mkdir /install
WORKDIR /install
COPY requirements.txt /requirements.txt
RUN pip install --prefix="/install" -r /requirements.txt

# Final image
FROM base
COPY --from=builder /install /usr/local
COPY src /app
WORKDIR /app
CMD python app.py --log-level DEBUG