from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

app = Flask(__name__)

DATA_FILE = Path(__file__).resolve().parent / "data" / "customers.json"

with DATA_FILE.open("r", encoding="utf-8") as file_handle:
    CUSTOMERS: list[dict[str, Any]] = json.load(file_handle)


def _parse_positive_int(value: str | None, default: int) -> int:
    """Parse a positive integer query parameter with a fallback default."""
    if value is None:
        return default

    parsed_value = int(value)
    if parsed_value < 1:
        raise ValueError("Value must be greater than zero.")
    return parsed_value


@app.get("/api/customers")
def get_customers() -> tuple[Any, int] | Any:
    """Return paginated customer data from the in-memory dataset."""
    try:
        page = _parse_positive_int(request.args.get("page"), 1)
        limit = _parse_positive_int(request.args.get("limit"), 10)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    start_index = (page - 1) * limit
    end_index = start_index + limit
    paginated_customers = CUSTOMERS[start_index:end_index]

    return jsonify(
        {
            "data": paginated_customers,
            "total": len(CUSTOMERS),
            "page": page,
            "limit": limit,
        }
    )


@app.get("/api/customers/<customer_id>")
def get_customer(customer_id: str) -> tuple[Any, int] | Any:
    """Return a single customer record by identifier."""
    customer = next(
        (record for record in CUSTOMERS if record["customer_id"] == customer_id),
        None,
    )
    if customer is None:
        return jsonify({"error": "Customer not found"}), 404
    return jsonify(customer)


@app.get("/api/health")
def health_check() -> Any:
    """Return a simple health status."""
    return jsonify({"status": "healthy"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
