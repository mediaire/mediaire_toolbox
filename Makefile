# no buildin rules and variables
MAKEFLAGS =+ -rR --warn-undefined-variables

PROJECT = mediaire_toolbox
IMAGE_BASE_NAME = $(PROJECT)
IMAGE_TAG_LATEST = latest
IMAGE_TAG = $(shell git describe --tags --always --dirty)

build:
	docker build -t $(IMAGE_BASE_NAME):$(IMAGE_TAG) \
	             -t $(IMAGE_BASE_NAME):$(IMAGE_TAG_LATEST) \
	             -f Dockerfile .

push:
	docker push $(IMAGE_BASE_NAME):$(IMAGE_TAG)
	docker push $(IMAGE_BASE_NAME):$(IMAGE_TAG_LATEST)

run:
	docker run $(IMAGE_BASE_NAME):$(IMAGE_TAG)

shell:
	docker run -it $(IMAGE_BASE_NAME):$(IMAGE_TAG) sh

test:
	docker run $(IMAGE_BASE_NAME):$(IMAGE_TAG) nosetests --with-coverage --cover-package=$(PROJECT) --cover-min-percentage=75 tests $(PROJECT)/*.py

lint:
	docker run $(IMAGE_BASE_NAME):$(IMAGE_TAG) flake8 $(PROJECT)

bandit:
	docker run $(IMAGE_BASE_NAME):$(IMAGE_TAG) bandit -r $(PROJECT)

safety:
	docker run $(IMAGE_BASE_NAME):$(IMAGE_TAG) safety check -r /src/requirements.txt

test_resource_manager: build
	docker run $(IMAGE_BASE_NAME):$(IMAGE_TAG) \
		nosetests --verbose \
			tests/test_redis_wq.py:TestRedisSlotWQ

test_resource_manager_local:
	pipenv run \
		nosetests --verbose --logging-level=INFO \
			tests/test_redis_wq.py:TestRedisSlotWQ

LOAD_SOCKET = $$(python3 -c 'import json; print(json.load(open("/tmp/redis.db.settings"))["unixsocket"])')

redis_monitor:
	redis-cli -s $(LOAD_SOCKET) monitor

redis_cli:
	redis-cli -s $(LOAD_SOCKET)
