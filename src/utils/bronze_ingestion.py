import json
from typing import Any, Generator, List


def load_cities(file_path: str) -> List[dict]:
    """Loads the list of cities from a JSON file."""
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def create_batches(items: List[Any], batch_size: int) -> Generator[List[Any], None, None]:
    """Splits a list of items into batches with a maximum size defined by batch_size."""
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]