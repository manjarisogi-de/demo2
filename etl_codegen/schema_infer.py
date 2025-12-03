import json
from typing import Dict, Any

def infer_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "double"
    return "string"


def infer_schema(obj: Dict[str, Any], prefix: str = "") -> Dict[str, str]:
    schema = {}

    for k, v in obj.items():
        col_name = f"{prefix}{k}" if prefix == "" else f"{prefix}.{k}"

        # Struct
        if isinstance(v, dict):
            nested = infer_schema(v, prefix=col_name)
            schema.update(nested)

        # Array detection
        elif isinstance(v, list):
            if len(v) > 0:
                if isinstance(v[0], dict):
                    schema[col_name] = "array_of_struct"
                else:
                    schema[col_name] = "array"
            else:
                schema[col_name] = "array"

        # Primitive
        else:
            schema[col_name] = infer_type(v)

    return schema


def infer_schema_from_samples(records):
    return infer_schema(records[0])


def flatten_record(record: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    flat = {}
    for k, v in record.items():
        col_name = f"{prefix}{k}" if prefix == "" else f"{prefix}.{k}"

        if isinstance(v, dict):
            flat.update(flatten_record(v, prefix=col_name))

        elif isinstance(v, list):
            flat[col_name] = v  # keep full list as preview

        else:
            flat[col_name] = v

    return flat