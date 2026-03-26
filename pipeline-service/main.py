from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import requests
from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from database import get_session, init_db
from models.customer import Customer
from services.ingestion import IngestionResult, ingest_customers

app = FastAPI(title="Customer Pipeline Service", version="1.0.0")


class CustomerResponse(BaseModel):
    """Serialized customer payload returned by the API."""

    customer_id: str
    first_name: str
    last_name: str
    email: str
    phone: str | None
    address: str | None
    date_of_birth: date | None
    account_balance: Decimal | None
    created_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class PaginatedCustomersResponse(BaseModel):
    """Paginated customer response format."""

    data: list[CustomerResponse]
    total: int
    page: int
    limit: int


class IngestionResponse(BaseModel):
    """Ingestion response payload."""

    status: str
    records_processed: int


@app.on_event("startup")
def startup_event() -> None:
    """Initialize the database schema at service startup."""
    init_db()


@app.post("/api/ingest", response_model=IngestionResponse)
def run_ingestion(session: Session = Depends(get_session)) -> IngestionResponse:
    """Trigger a full ingestion from the mock server into PostgreSQL."""
    try:
        result: IngestionResult = ingest_customers(session)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch source data: {exc}") from exc
    except (SQLAlchemyError, ValueError, KeyError) as exc:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {exc}") from exc

    return IngestionResponse(status="success", records_processed=result.records_processed)


@app.get("/api/customers", response_model=PaginatedCustomersResponse)
def list_customers(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> PaginatedCustomersResponse:
    """Return paginated customers from PostgreSQL."""
    total = session.scalar(select(func.count()).select_from(Customer))
    offset = (page - 1) * limit

    customers = session.scalars(
        select(Customer).order_by(Customer.created_at.desc(), Customer.customer_id).offset(offset).limit(limit)
    ).all()

    return PaginatedCustomersResponse(
        data=[CustomerResponse.model_validate(customer) for customer in customers],
        total=int(total or 0),
        page=page,
        limit=limit,
    )


@app.get("/api/customers/{customer_id}", response_model=CustomerResponse)
def get_customer(customer_id: str, session: Session = Depends(get_session)) -> CustomerResponse:
    """Return a single customer from PostgreSQL by identifier."""
    customer = session.get(Customer, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Customer not found")
    return CustomerResponse.model_validate(customer)


@app.get("/api/health")
def health_check() -> dict[str, Any]:
    """Return service health for container checks and manual verification."""
    return {"status": "healthy"}
