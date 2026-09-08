FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY queryapi/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY queryapi/ ./

COPY common/ ./common/

ENV PYTHONUNBUFFERED=1

# Non-root
RUN useradd --create-home --uid 1000 queryapi && chown -R queryapi:queryapi /app
USER queryapi

CMD ["gunicorn", "queryapi_project.wsgi:application", "--bind", "0.0.0.0:8091", "--workers", "2"]
