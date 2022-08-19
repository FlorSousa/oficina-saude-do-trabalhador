#Pega a imagem do site oficial do docker
FROM python:3.8.1-alpine

ENV PYTHONUBBUFFERED 1

#Cria uma pasta na raiz do servidor chamada /code
RUN mkdir /code

#Define o diretorio /code como o em uso
WORKDIR /code

#Copia os nossos arquivos para dentro da pasta /code que fica dentro do docker
COPY . /code/

#Atualiza todos os pacotes do kernel do linux
RUN apk update

#instalar pacote libjpeg
RUN apk add libjpeg

#Instala uma serie de pacotes necessarios
RUN apk add --no-cache libstdc++  && \
    apk add py3-pip gcc musl-dev python3-dev pango zlib-dev jpeg-dev openjpeg-dev g++ libffi-dev && \
 apk add --no-cache postgresql-libs && \
 apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev linux-headers build-base jpeg-dev zlib-dev libjpeg-turbo-dev python-dev \
			libffi-dev libxml2-dev libwebp-dev  && \
 python3 -m pip install -r requirements.txt --no-cache-dir && \
 apk --purge del .build-deps

#Instala o comando wait do docker-compose
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.9.0/wait /wait

#Define o comando wait baixado como executavel
RUN chmod +x /wait
