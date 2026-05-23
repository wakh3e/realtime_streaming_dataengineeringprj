#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
while ! nc -z postgres 5432; do
  sleep 1
done
echo "PostgreSQL is ready!"

# Initialize Airflow database if not already initialized
echo "Initializing Airflow database..."
airflow db init

# Create default admin user if not exists
airflow users create \
  --username airflow \
  --password airflow \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

# Execute the main command passed as arguments
exec airflow "$@"
