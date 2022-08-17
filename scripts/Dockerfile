FROM python:3.8.1-alpine

ENV PYTHONUBBUFFERED 1

RUN mkdir /code
WORKDIR /code

COPY . /code/

RUN apk update

RUN apk add libjpeg

RUN apk add --no-cache libstdc++  && \
    apk add py3-pip gcc musl-dev python3-dev pango zlib-dev jpeg-dev openjpeg-dev g++ libffi-dev && \
 apk add --no-cache postgresql-libs && \
 apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev linux-headers build-base jpeg-dev zlib-dev libjpeg-turbo-dev python-dev \
			libffi-dev libxml2-dev libwebp-dev  && \
 python3 -m pip install -r requirements.txt --no-cache-dir && \
 apk --purge del .build-deps

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.9.0/wait /wait
RUN chmod +x /wait