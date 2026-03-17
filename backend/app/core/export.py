import csv
import io
import json
from typing import Any, AsyncIterator

from fastapi.responses import StreamingResponse


def stream_csv(data: list[dict[str, Any]], columns: list[str]) -> StreamingResponse:
    """Stream data as a CSV download."""

    def generate() -> Any:
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        for row in data:
            writer.writerow(row)
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=export.csv"},
    )


def stream_json(data: list[dict[str, Any]]) -> StreamingResponse:
    """Stream data as a JSON array download."""

    def generate() -> Any:
        yield "["
        for i, item in enumerate(data):
            if i > 0:
                yield ","
            yield json.dumps(item, ensure_ascii=False, default=str)
        yield "]"

    return StreamingResponse(
        generate(),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=export.json"},
    )
