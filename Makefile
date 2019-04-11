docker_local_server_args := \
	--rm \
	--env AWS_PROFILE=s3-read \
	--volume ~/.aws:/root/.aws \
	--publish 8000:8000 \


build:
	 docker build -t ann-serve .

run-local:
	docker run -it $(docker_local_server_args) ann-serve
