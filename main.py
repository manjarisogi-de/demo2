import json
import sys
import os

from etl_codegen.schema_infer import infer_schema_from_samples, flatten_record
from etl_codegen.codegen import generate_pyspark_code


def detect_array_columns(schema):
    # Return only array-of-struct columns ("events", etc.)
    return [col for col, t in schema.items() if t == "array_of_struct"]


def generate_llm_prompt(schema, flat_sample, dq_suggestions, generated_code, array_cols):
    schema_lines = []
    for col, typ in schema.items():
        schema_lines.append(f"- {col}: {typ}")
    schema_text = "\n".join(schema_lines)

    array_info = ", ".join(array_cols) if array_cols else "None"

    prompt = f"""
Generate a complete PySpark ETL script from scratch based on the schema below.

Requirements:
- Use clean, production-quality PySpark
- Use col() for all column references
- Use alias() to flatten nested fields
- Normalize timestamps using to_timestamp()
- Explode ONLY these array-of-struct fields (if present): {array_info}
- Cast numeric fields to correct types
- Apply simple non-null filters
- Write Parquet with overwrite mode and snappy compression
- No placeholder column names
- No invented fields

Schema:
{schema_text}

Output only the PySpark code. Do not repeat the instructions.
Begin your answer with 'from pyspark.sql'.
"""
    return prompt.strip()


def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <json_path>")
        sys.exit(1)

    sample_path = sys.argv[1]
    if not os.path.exists(sample_path):
        print(f"Error: File {sample_path} not found.")
        sys.exit(1)

    with open(sample_path, "r") as f:
        content = f.read().strip()
        if content.startswith("["):
            records = json.loads(content)
        else:
            records = [json.loads(content)]

    schema = infer_schema_from_samples(records)
    flat_sample = flatten_record(records[0])
    array_cols = detect_array_columns(schema)

    dq_suggestions = []
    for col_name, col_type in schema.items():
        clean = col_name.replace(".", "_")
        if "id" in col_name.lower():
            dq_suggestions.append(f"- `{clean}` should not be null")
        if "ts" in col_name.lower() or "date" in col_name.lower():
            dq_suggestions.append(f"- `{clean}` should be a valid timestamp")
        if col_type in ("int", "double"):
            dq_suggestions.append(f"- `{clean}` should not be negative")

    generated_code = generate_pyspark_code(schema)

    llm_prompt = generate_llm_prompt(schema, flat_sample, dq_suggestions, generated_code, array_cols)

    with open("llm_prompt.txt", "w") as f:
        f.write(llm_prompt)

    print("llm_prompt.txt generated successfully")


if __name__ == "__main__":
    main()