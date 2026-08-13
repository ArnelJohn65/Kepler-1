FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends bash && \
    rm -rf /var/lib/apt/lists/*

COPY data/ /app/data/

RUN mkdir -p /app

WORKDIR /app

CMD ["bash", "/app/data/run.sh"]
