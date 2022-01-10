FROM python:3.5-alpine

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# for debugging
RUN apk add sqlite
# for --numprocesses=auto
RUN apk add py3-psutil

COPY requirements.txt /src/requirements.txt

WORKDIR /src
RUN pip install -r requirements.txt

COPY . /src
RUN pip install -e .
