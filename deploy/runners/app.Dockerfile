FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "livepeer-gateway @ git+https://github.com/eliteprox/livepeer-python-gateway.git@3250c08cb6424d339614f1e9095064fbe7e6da6e"

WORKDIR /app
COPY runner.py .
EXPOSE 8989
ENTRYPOINT ["python", "runner.py"]
