# syntax=docker/dockerfile:1.4
FROM python:3.12-alpine as builder

WORKDIR /app

RUN apk add git

COPY requirements.txt /app

RUN  pip3 install -r requirements.txt

FROM python:3.12-alpine AS run

WORKDIR /app

RUN apk add tzdata

COPY --from=builder /usr/local /usr/local
COPY src/* /app/

CMD ["python3",  "app.py" ]