# spark-etl-framework

## Setup local

```bash
poetry install
poetry run pytest
poetry run pytest --cov=etl_framework
poetry run black .
poetry run isort .
poetry run pre-commit install
poetry run pre-commit run --all-files
poetry run cz commit
```

## Setup local dev

```bash
poetry install --with dev
```