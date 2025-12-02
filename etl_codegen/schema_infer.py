import json
from typing import Dict, Any

def infer_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "double"
    # naive check for timestamp-ish strings could be added later
    return "string"

def flatten_record(record: Dict[str, Any], prefix: str = "") -> Dict[str, str]:
    """
    Flattens a single JSON object one level deep.
    Returns {column_name: spark_type_string}
    """
    flat = {}
    for k, v in record.items():
        col_name = f"{prefix}{k}" if prefix == "" else f"{prefix}.{k}"
        if isinstance(v, dict):
            # one level nested object – flatten with dot notation
            nested = flatten_record(v, prefix=col_name)
            flat.update(nested)
        else:
            flat[col_name] = infer_type(v)
    return flat

def infer_schema_from_samples(records):
    """
    records: list of dicts
    """
    if not records:
        return {}

    # For MVP, just inspect first record
    first = records[0]
    return flatten_record(first)
