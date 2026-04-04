PYTEST = python3 -m pytest
RUFF = ruff
SRC = sqlalchemy_altibase
TESTS = test

.PHONY: lint format test test-e2e-docker clean

lint:
	$(RUFF) check $(SRC)/ $(TESTS)/
	$(RUFF) format --check $(SRC)/ $(TESTS)/

format:
	$(RUFF) check --fix $(SRC)/ $(TESTS)/
	$(RUFF) format $(SRC)/ $(TESTS)/

test:
	$(PYTEST) $(TESTS)/ -v \
		--cov=$(SRC) \
		--cov-report=term-missing \
		--cov-fail-under=95

test-e2e-docker:
	docker compose -f docker-compose.e2e.yml up --build --abort-on-container-exit --exit-code-from e2e

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache/ .coverage .ruff_cache/ __pycache__/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true
