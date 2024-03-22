FROM python:3.11-slim

RUN pip install poetry

WORKDIR /usr/src/app

COPY ./ /usr/src/app

RUN poetry install

WORKDIR /usr/src/app/bdi_api

EXPOSE 8000

CMD [ "poetry", "run", "uvicorn", "bdi_api.app:app", "--host", "0.0.0.0"]