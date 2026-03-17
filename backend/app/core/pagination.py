from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PaginationParams(BaseModel):
    """Input parameters for paginated requests."""

    cursor: str | None = Field(default=None, description="Cursor for next page")
    limit: int = Field(default=50, ge=1, le=250, description="Number of items per page")
    page: int | None = Field(default=None, ge=1, description="Page number (1-based)")


class PaginatedResponse(BaseModel):
    """Standard paginated response envelope."""

    items: list[Any] = Field(default_factory=list, description="Result items")
    next_cursor: str | None = Field(default=None, description="Cursor for next page")
    total_count: int | None = Field(default=None, description="Total number of items if known")


def build_paginated_response(
    items: list[Any],
    next_cursor: str | None = None,
    total_count: int | None = None,
) -> PaginatedResponse:
    """Build a standardized paginated response."""
    return PaginatedResponse(
        items=items,
        next_cursor=next_cursor,
        total_count=total_count,
    )
