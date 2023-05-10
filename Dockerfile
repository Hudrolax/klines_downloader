FROM python:3.11.1-alpine3.17
LABEL maintainer="hud.net.ru"

ENV PYTHONUNBUFFERED 1

WORKDIR /app
ARG DEV=false

COPY . .

ENV PYTHONPATH=/app

RUN python -m venv /py && \
  /py/bin/pip install --upgrade pip && \
  apk add --update --no-cache --virtual .tmp-build-deps \
  build-base musl-dev zlib zlib-dev linux-headers libffi-dev && \
  /py/bin/pip install -r requirements.txt && \
  if [ $DEV = "true" ]; \
  then /py/bin/pip install -r requirements.dev.txt ; \
  fi && \
  apk del .tmp-build-deps

RUN ["chmod", "+x", "/app/run.sh"]

ENV PATH="/app:/py/bin:$PATH"

ENV NAME Download_Klines_Service

CMD ["run.sh"]
