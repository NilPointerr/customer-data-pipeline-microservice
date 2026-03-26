from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from math import ceil
from typing import Any

import requests
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from models.customer import Customer

MOCK_SERVER_URL = "http://mock-server:5000/api/customers"
DEFAULT_PAGE_SIZE = 10
REQUEST_TIMEOUT_SECONDS = 10


@dataclass(frozen=True)
class IngestionResult:
    """Summary of a completed ingestion run."""

    records_processed: int


def _fetch_customer_page(page: int, limit: int) -> dict[str, Any]:
    """Fetch a single page of customers from the mock server."""
    response = requests.get(
        MOCK_SERVER_URL,
        params={"page": page, "limit": limit},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected response format from mock server.")
    return payload


def _fetch_all_customers(limit: int = DEFAULT_PAGE_SIZE) -> list[dict[str, Any]]:
    """Fetch every customer record from the paginated mock server endpoint."""
    first_page = _fetch_customer_page(page=1, limit=limit)
    total = int(first_page.get("total", 0))
    first_page_records = first_page.get("data", [])
    if not isinstance(first_page_records, list):
        raise ValueError("Unexpected paginated response from mock server.")

    records = list(first_page_records)

    if total <= len(records):
        return records

    total_pages = ceil(total / limit)
    for page in range(2, total_pages + 1):
        payload = _fetch_customer_page(page=page, limit=limit)
        page_records = payload.get("data", [])
        if not isinstance(page_records, list):
            raise ValueError("Unexpected paginated response from mock server.")
        records.extend(page_records)

    return records


def _transform_customer(record: dict[str, Any]) -> dict[str, Any]:
    """Normalize incoming API payloads to SQLAlchemy-compatible values."""
    created_at_value = record.get("created_at")
    normalized_created_at = None
    if isinstance(created_at_value, str):
        normalized_created_at = datetime.fromisoformat(created_at_value.replace("Z", "+00:00"))

    date_of_birth_value = record.get("date_of_birth")
    normalized_dob = None
    if isinstance(date_of_birth_value, str):
        normalized_dob = date.fromisoformat(date_of_birth_value)

    account_balance_value = record.get("account_balance")
    normalized_balance = None
    if account_balance_value is not None:
        normalized_balance = Decimal(str(account_balance_value))

    return {
        "customer_id": str(record["customer_id"]),
        "first_name": str(record["first_name"]),
        "last_name": str(record["last_name"]),
        "email": str(record["email"]),
        "phone": str(record["phone"]) if record.get("phone") is not None else None,
        "address": str(record["address"]) if record.get("address") is not None else None,
        "date_of_birth": normalized_dob,
        "account_balance": normalized_balance,
        "created_at": normalized_created_at,
    }


def ingest_customers(session: Session) -> IngestionResult:
    """Fetch, transform, and upsert customers into PostgreSQL."""
    records = _fetch_all_customers()
    transformed_records = [_transform_customer(record) for record in records]

    for record in transformed_records:
        statement = insert(Customer).values(**record)
        upsert_statement = statement.on_conflict_do_update(
            index_elements=[Customer.customer_id],
            set_={
                "first_name": statement.excluded.first_name,
                "last_name": statement.excluded.last_name,
                "email": statement.excluded.email,
                "phone": statement.excluded.phone,
                "address": statement.excluded.address,
                "date_of_birth": statement.excluded.date_of_birth,
                "account_balance": statement.excluded.account_balance,
                "created_at": statement.excluded.created_at,
            },
        )
        session.execute(upsert_statement)

    session.commit()
    return IngestionResult(records_processed=len(transformed_records))
