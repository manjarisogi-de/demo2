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

def flatten_record(record, prefix=""):
    flat = {}
    for k, v in record.items():
        col_name = f"{prefix}{k}" if prefix == "" else f"{prefix}.{k}"

        # If nested dict → flatten it recursively
        if isinstance(v, dict):
            nested = flatten_record(v, prefix=col_name)
            flat.update(nested)

        # If list → KEEP THE RAW VALUE (we will rely on explode later)
        elif isinstance(v, list):
            flat[col_name] = v  # don't replace with types

        else:
            # KEEP THE RAW VALUE (NOT infer_type)
            flat[col_name] = v

    return flat


def infer_schema_from_samples(records):
    # Infer schema based on first record only
    record = records[0]
    return infer_schema(record)


def infer_schema(obj, prefix=""):
    schema = {}
    for k, v in obj.items():
        col_name = f"{prefix}{k}" if prefix == "" else f"{prefix}.{k}"
        if isinstance(v, dict):
            nested = infer_schema(v, prefix=col_name)
            schema.update(nested)
        elif isinstance(v, list):
            # simple assumption: array of objects or primitives
            if len(v) > 0 and isinstance(v[0], dict):
                schema[col_name] = "array_of_struct"
            else:
                schema[col_name] = "array"
        else:
            schema[col_name] = infer_type(v)
    return schema