docker_local_server_args := \
	--rm \
	--env AWS_PROFILE=s3-read \
	--volume ~/.aws:/root/.aws \
	--publish 8000:8000 \

GIT_HASH = $(shell git rev-parse --short HEAD)

ifndef TAG  # if kwarg `TAG` not specified
    TAG = $(GIT_HASH)
endif


build:
	docker build -t ann-serve:latest .

run-local:
	docker run -it $(docker_local_server_args) ann-serve:latest

publish:
	docker tag ann-serve:latest 2jason/ann-serve:$(TAG)
	docker push 2jason/ann-serve:$(TAG)