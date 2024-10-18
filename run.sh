#!/bin/bash
pip install -r requirements.txt

# Start the Docker containers
docker-compose up -d

docker exec -it university_postgres psql -U admin -d university

# Wait for PostgreSQL to start
echo "Waiting for PostgreSQL to start..."

python psql_data.py

python data_migration.py

