.PHONY: proto test lint type sim

proto:
	python3 proto/gen_proto.py

test: proto
	python3 -m pytest -q

lint:
	python3 -m ruff check .
	python3 -m ruff format --check .

type: proto
	python3 -m mypy minikv sim
