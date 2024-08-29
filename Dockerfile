FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /app
RUN mkdir /app/output
WORKDIR /app

COPY ./requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/.

ENTRYPOINT ["python", "main.py"]