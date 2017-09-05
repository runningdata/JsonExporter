FROM python:2.7.13-alpine3.6
MAINTAINER  Will Chen <willcup@163.com>

RUN echo "http://mirrors.aliyun.com/alpine/v3.4/main/" > /etc/apk/repositories && apk update && apk upgrade && \
    apk add --no-cache bash git openssh

COPY /requirements.txt .
RUN    pip install --no-cache-dir -r requirements.txt

COPY /entrypoint.sh .
EXPOSE 1234
ENTRYPOINT ["sh", "entrypoint.sh"]