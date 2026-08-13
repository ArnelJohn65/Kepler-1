FROM python:3.11-slim

RUN mkdir -p /app
RUN pip install --no-cache-dir pyarrow==17.0.0 pytest==9.1.1 pytest-json-ctrf==0.5.2

WORKDIR /tests
COPY . /tests/
