.PHONY: test run docker
test:
	pytest -q
run:
	uvicorn src.api:app --reload --port 8001
docker:
	docker compose up --build
