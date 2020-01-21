docker_local_server_args := \
	--rm \
	--env AWS_PROFILE=s3-read \
	--volume ~/.aws:/root/.aws \
	--publish 8000:8000 \


NAME = ann-serve
PORT_CONTAINER = 8000
PORT_HOST = 8000
REGION = us-east-1
REGISTRY_URL = 2jason
GIT_HASH = $(shell git rev-parse --short HEAD)

ifndef TAG  # if kwarg `TAG` not specified
	TAG = $(GIT_HASH)
endif

# Local Targets
build:
	docker build -t ${NAME}:${TAG} .

docker_local_args := \
	--rm \
	--volume ~/.aws:/root/.aws

run-local:
	docker run -it \
		$(docker_local_args) \
		-p ${PORT_HOST}:${PORT_CONTAINER} \
		${NAME}:${TAG}

deploy-local-k8s:
	kubectl apply -f k8s/local

# Remote Targets
ecs-login:
	$(shell aws ecr get-login --region ${REGION} --no-include-email)

create-repo: ecs-login
	aws ecr create-repository --repository-name $(NAME)

build-remote:
	docker build \
		-t $(REGISTRY_URL)/$(NAME):$(TAG) .

publish: build-remote
	docker push $(REGISTRY_URL)/$(NAME):$(TAG)
