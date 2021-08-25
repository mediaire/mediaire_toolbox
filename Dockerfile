FROM python:3.5-alpine

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# needed for building `redislite` wheel
RUN apk add build-base linux-headers

COPY requirements.txt /src/requirements.txt

WORKDIR /src
RUN pip install -r requirements.txt

COPY . /src
RUN pip install -e .
