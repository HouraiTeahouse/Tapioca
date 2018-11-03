FROM python:alpine as base
FROM base as builder
RUN apk add --no-cache protobuf-dev build-base
COPY requirements.txt /
RUN pip install --prefix /install -r /requirements.txt
WORKDIR /app
COPY . /app
RUN protoc $(find . -type f -regex ".*\.proto") --python_out=.

FROM base
COPY --from=builder /install /usr/local
COPY --from=builder /app /app
WORKDIR /app
CMD ["python", "-m", "tapioca", "run", "server"]
