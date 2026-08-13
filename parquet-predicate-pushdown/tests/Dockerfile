FROM python:3.11-slim

RUN mkdir -p /app /tests/data

RUN pip install --no-cache-dir pyarrow==17.0.0 pytest==9.1.1 pytest-json-ctrf==0.5.2

WORKDIR /tests
COPY . /tests/

# Generate pristine dataset at build time — deterministic, seeded
RUN python /tests/generate.py

CMD ["bash"]
