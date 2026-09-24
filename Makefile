PYTHON ?= .venv/bin/python
export PYTHONPATH := $(CURDIR)

.PHONY: up down test lint typecheck migrate seed

.env:
	cp .env.example .env

up: .env
	docker compose up --build -d

down:
	docker compose down

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .

typecheck:
	$(PYTHON) -m mypy

migrate:
	$(PYTHON) -m alembic upgrade head

seed:
	$(PYTHON) -m store.seed
