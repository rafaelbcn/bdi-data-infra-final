FROM python:3.11-slim

EXPOSE 8080
ENV APP_DIR /opt/app
ENV BDI_LOCAL_DIR /opt/data
WORKDIR $APP_DIR

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gcc \
    && curl -sSL https://install.python-poetry.org | POETRY_HOME=/usr/local/ python3 \
    && poetry config virtualenvs.in-project true

COPY poetry.lock pyproject.toml Makefile $APP_DIR/
COPY bdi_api $APP_DIR/

RUN poetry install --no-dev --no-root --no-interaction --no-ansi


CMD ["uvicorn", "bdi_api.app:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8080"]