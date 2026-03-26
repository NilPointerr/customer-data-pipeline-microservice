# Customer Data Pipeline Microservice

This project contains a Dockerized three-tier setup:

- `mock-server`: Flask API that serves mock customer data from a JSON file
- `pipeline-service`: FastAPI service that ingests customer data into PostgreSQL and exposes it
- `postgres`: PostgreSQL 15 database

## Start the project

```bash
docker-compose up --build -d
```

## Test commands

Check the mock server health:

```bash
curl http://localhost:5000/api/health
```

Fetch paginated customers from the mock server:

```bash
curl "http://localhost:5000/api/customers?page=1&limit=5"
```

Fetch one customer from the mock server:

```bash
curl http://localhost:5000/api/customers/<customer_id>
```

Trigger ingestion into PostgreSQL:

```bash
curl -X POST http://localhost:8000/api/ingest
```

Fetch paginated customers from the pipeline service:

```bash
curl "http://localhost:8000/api/customers?page=1&limit=10"
```

Fetch one customer from the pipeline service:

```bash
curl http://localhost:8000/api/customers/<customer_id>
```

## Notes

- The pipeline service reads its database connection from `DATABASE_URL`.
- Docker networking uses the service hostnames `mock-server` and `postgres`.
