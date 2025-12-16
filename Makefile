.PHONY: dev-install test test-all test-cov lint format typecheck pre-commit clean clean-all

dev-install:
	uv sync --all-extras
	uv run pre-commit install

test:
	uv run pytest tests/unit -v

test-all:
	uv run pytest tests/ -v

test-cov:
	uv run pytest tests/ -v --cov=src/chtc_memory_analyzer --cov-report=html

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run ty check .

pre-commit:
	uv run pre-commit run --all-files

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .ruff_cache htmlcov .coverage

clean-all: clean
	rm -rf .venv/
