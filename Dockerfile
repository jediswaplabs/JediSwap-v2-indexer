FROM python:3.11.3-buster

WORKDIR /app

COPY server .

WORKDIR /app/server

COPY env_goerli .
COPY env_mainnet .

RUN python3 -m pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev

ENTRYPOINT [ "server" ]