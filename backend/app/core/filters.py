"""Post-extraction filtering logic for extracted data items."""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import BaseModel, ValidationError

logger = logging.getLogger("ecommerce_data_extractor.filters")


class FilterDefinition(BaseModel):
    column: str
    operator: str
    value: str


# Operators grouped by type
DATE_OPERATORS = {"last_n_days", "last_n_hours", "date_range", "after", "before"}
STRING_OPERATORS = {"equals", "eq", "contains", "starts_with", "not_equals"}
NUMBER_OPERATORS = {"equals", "eq", "gte", "lte", "range"}
BOOLEAN_OPERATORS = {"equals", "eq"}

ALL_OPERATORS = DATE_OPERATORS | STRING_OPERATORS | NUMBER_OPERATORS | BOOLEAN_OPERATORS

# Field types that map to each filter category
_DATE_TYPES = {"date", "datetime"}
_NUMBER_TYPES = {"number", "integer", "float"}
_BOOLEAN_TYPES = {"boolean"}


def parse_filters(filters_json: str) -> list[FilterDefinition]:
    """Parse a JSON string into a list of FilterDefinition objects."""
    try:
        raw = json.loads(filters_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid filters JSON: {e}") from e

    if not isinstance(raw, list):
        raise ValueError("Filters must be a JSON array")

    filters: list[FilterDefinition] = []
    for item in raw:
        try:
            filters.append(FilterDefinition(**item))
        except (ValidationError, TypeError) as e:
            raise ValueError(f"Invalid filter definition: {e}") from e

    return filters


def _parse_datetime(value: Any) -> datetime | None:
    """Parse a datetime value from various formats, returning timezone-aware datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Try ISO 8601 parsing
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            # Try common formats
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(s, fmt)
                    break
                except ValueError:
                    continue
            else:
                logger.warning("Unable to parse datetime: %s", s)
                return None
    else:
        return None

    # Ensure timezone-aware (assume UTC if naive)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _to_number(value: Any) -> float | None:
    """Try to convert a value to a number."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _to_bool(value: Any) -> bool | None:
    """Convert a value to a boolean, handling string representations."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lower = value.strip().lower()
        if lower in ("true", "1", "yes"):
            return True
        if lower in ("false", "0", "no"):
            return False
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    return None


def _apply_date_filter(item_value: Any, operator: str, filter_value: str) -> bool:
    """Apply a date-type filter. Returns True if the item passes."""
    item_dt = _parse_datetime(item_value)
    if item_dt is None:
        return False

    now = datetime.now(timezone.utc)

    if operator == "last_n_days":
        n = int(filter_value)
        cutoff = now - timedelta(days=n)
        return item_dt >= cutoff

    elif operator == "last_n_hours":
        n = int(filter_value)
        cutoff = now - timedelta(hours=n)
        return item_dt >= cutoff

    elif operator == "date_range":
        parts = [p.strip() for p in filter_value.split(",")]
        if len(parts) != 2:
            raise ValueError("date_range value must be 'start,end'")
        start_dt = _parse_datetime(parts[0])
        end_dt = _parse_datetime(parts[1])
        if start_dt is None or end_dt is None:
            raise ValueError(f"Invalid date_range dates: {filter_value}")
        return start_dt <= item_dt <= end_dt

    elif operator == "after":
        threshold = _parse_datetime(filter_value)
        if threshold is None:
            raise ValueError(f"Invalid 'after' date: {filter_value}")
        return item_dt > threshold

    elif operator == "before":
        threshold = _parse_datetime(filter_value)
        if threshold is None:
            raise ValueError(f"Invalid 'before' date: {filter_value}")
        return item_dt < threshold

    return False


def _apply_string_filter(item_value: Any, operator: str, filter_value: str) -> bool:
    """Apply a string-type filter. Returns True if the item passes."""
    if item_value is None:
        return operator == "not_equals"

    s = str(item_value)

    if operator in ("equals", "eq"):
        return s == filter_value
    elif operator == "contains":
        return filter_value in s
    elif operator == "starts_with":
        return s.startswith(filter_value)
    elif operator == "not_equals":
        return s != filter_value

    return False


def _apply_number_filter(item_value: Any, operator: str, filter_value: str) -> bool:
    """Apply a number-type filter. Returns True if the item passes."""
    num = _to_number(item_value)
    if num is None:
        return False

    if operator in ("equals", "eq"):
        return num == float(filter_value)
    elif operator == "gte":
        return num >= float(filter_value)
    elif operator == "lte":
        return num <= float(filter_value)
    elif operator == "range":
        parts = [p.strip() for p in filter_value.split(",")]
        if len(parts) != 2:
            raise ValueError("range value must be 'min,max'")
        return float(parts[0]) <= num <= float(parts[1])

    return False


def _apply_boolean_filter(item_value: Any, operator: str, filter_value: str) -> bool:
    """Apply a boolean-type filter. Returns True if the item passes."""
    item_bool = _to_bool(item_value)
    filter_bool = _to_bool(filter_value)

    if item_bool is None or filter_bool is None:
        return False

    if operator in ("equals", "eq"):
        return item_bool == filter_bool

    return False


def _get_nested_value(item: dict, column: str) -> Any:
    """Get a value from a possibly nested dict using dot notation."""
    keys = column.split(".")
    current = item
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
    return current


def _resolve_field_type(column: str, field_types: dict[str, str]) -> str:
    """Resolve the field type for a column from the schema type map.

    Returns 'string' as default if the column is not found.
    """
    return field_types.get(column, "string")


def _dispatch_filter(
    item_value: Any, operator: str, filter_value: str, field_type: str,
) -> bool:
    """Dispatch to the correct filter function based on field type."""
    if field_type in _DATE_TYPES:
        return _apply_date_filter(item_value, operator, filter_value)
    if field_type in _NUMBER_TYPES:
        return _apply_number_filter(item_value, operator, filter_value)
    if field_type in _BOOLEAN_TYPES:
        return _apply_boolean_filter(item_value, operator, filter_value)

    # For string type or unknown types, fall back to operator-based dispatch
    # to keep backward compatibility with date operators on untyped fields
    if operator in DATE_OPERATORS:
        return _apply_date_filter(item_value, operator, filter_value)
    return _apply_string_filter(item_value, operator, filter_value)


def apply_filters(
    items: list[dict],
    filters: list[FilterDefinition],
    field_types: dict[str, str] | None = None,
) -> list[dict]:
    """Apply all filters to a list of items, returning only matching items.

    Args:
        items: List of data records to filter.
        filters: List of filter definitions to apply.
        field_types: Optional mapping of column name -> type string
                     (e.g. ``{"price": "number", "createdAt": "datetime"}``).
                     When provided, filtering is dispatched by field type
                     instead of operator name, ensuring correct type coercion.
    """
    if not filters:
        return items

    if field_types is None:
        field_types = {}

    result = []
    for item in items:
        passes_all = True
        for f in filters:
            item_value = _get_nested_value(item, f.column)
            op = f.operator
            ftype = _resolve_field_type(f.column, field_types)

            try:
                passed = _dispatch_filter(item_value, op, f.value, ftype)
            except (ValueError, TypeError) as e:
                logger.warning("Filter error for column=%s op=%s type=%s: %s", f.column, op, ftype, e)
                passed = False

            if not passed:
                passes_all = False
                break

        if passes_all:
            result.append(item)

    logger.info("Filtering: %d items -> %d items after %d filter(s)", len(items), len(result), len(filters))
    return result
