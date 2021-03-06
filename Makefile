docker_image := iomete/iomete_mongodb_sync
docker_tag := 0.2.2

setup:
	python3 -m venv .env
	source .env/bin/activate
	pip install -e ."[dev]"

test:
	pytest --capture=no --log-cli-level=INFO

docker-build:
	# Run this for one time: docker buildx create --use
	docker build -f docker/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 --push -f docker/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}
