FROM python:3.11-slim

RUN pip install --no-cache-dir pyarrow==17.0.0 pandas==2.2.2

WORKDIR /app

COPY data/ /app/data/
COPY engine/ /app/engine/

CMD ["bash"]
