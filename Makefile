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
	docker run $(IMAGE_BASE_NAME):$(IMAGE_TAG) \
		pytest \
			--color=yes \
			--cov=$(PROJECT) --cov-fail-under=75 \
			--numprocesses auto \
			tests \
			$(PROJECT)/*.py \

lint:
	docker run $(IMAGE_BASE_NAME):$(IMAGE_TAG) flake8 $(PROJECT)

bandit:
	docker run $(IMAGE_BASE_NAME):$(IMAGE_TAG) bandit -r $(PROJECT)

safety:
	docker run $(IMAGE_BASE_NAME):$(IMAGE_TAG) safety check -r /src/requirements.txt
