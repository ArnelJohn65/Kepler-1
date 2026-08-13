FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends bash && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyarrow==16.1.0 pandas==2.2.2

COPY data/ /app/data/

RUN mkdir -p /app

WORKDIR /app

CMD ["bash", "/app/data/run.sh"]
