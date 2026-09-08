FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY keyapi/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY keyapi/ ./

# The postprocessing algorithms registry, to validate
# function_name/inputs/params
COPY common/ ./common/

# KEYAPI__SECRET_KEY has no runtime fallback
RUN KEYAPI__SECRET_KEY=build-time-only-unused python manage.py collectstatic --noinput

ENV PYTHONUNBUFFERED=1

# Non-root user
RUN useradd --create-home --uid 1000 keyapi && chown -R keyapi:keyapi /app
USER keyapi

# Runs migrations
CMD ["sh", "-c", "python manage.py migrate --noinput && python manage.py seed_from_env && gunicorn keyapi_project.wsgi:application --bind 0.0.0.0:8090 --workers 2"]
