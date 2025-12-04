FROM python:3.11-slim
WORKDIR /app

# system deps
RUN apt-get update && apt-get install -y --no-install-recommends gcc git libpq-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . /app

ENV PYTHONUNBUFFERED=1
ENV POSTGRES_URI=${POSTGRES_URI}

CMD ["python", "main.py"]
