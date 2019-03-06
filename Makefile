docker_local_server_args := \
	--rm \
	--env AWS_PROFILE=jason \
	--volume ~/.aws:/root/.aws \
	--publish 8000:8000 \


build:
	 docker build -t crap .

run-local:
	docker run -it $(docker_local_server_args) crap
