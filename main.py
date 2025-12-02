import json
import sys
import os

from etl_codegen.schema_infer import infer_schema_from_samples, flatten_record
from etl_codegen.codegen import generate_pyspark_code


def generate_llm_prompt(schema, flat_sample, dq_suggestions, generated_code):
    schema_json = json.dumps(schema, indent=2)
    flat_sample_json = json.dumps(flat_sample, indent=2)
    dq_text = "\n".join(dq_suggestions)

    prompt = f"""
You are an expert Data Engineer. Below is everything you need to refine and improve an ETL pipeline.

=== 1. Inferred Flattened Schema ===
{schema_json}

=== 2. Draft PySpark ETL Code (Auto-Generated) ===
{generated_code}

=== 3. Sample Flattened Row ===
{flat_sample_json}

=== 4. Suggested Data Quality Checks ===
{dq_text}

=== 5. schema_output.json ===
{schema_json}

Your task:
- Improve and productionize the ETL pipeline
- Handle arrays (explode)
- Normalize timestamps
- Apply DQ checks
- Add comments and best practices
- Suggest partitioning strategy
- Optimize for Spark + Glue performance

Return a clean, production-ready PySpark ETL script.
"""
    return prompt


def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <path_to_json>")
        sys.exit(1)

    sample_path = sys.argv[1]

    if not os.path.exists(sample_path):
        print(f"Error: File {sample_path} not found.")
        sys.exit(1)

    # Read JSON sample
    with open(sample_path, "r") as f:
        content = f.read().strip()
        if content.startswith("["):
            records = json.loads(content)
        else:
            records = [json.loads(content)]

    # Infer schema
    schema = infer_schema_from_samples(records)
    print("\n# Inferred schema:")
    for k, v in schema.items():
        print(f"#   {k}: {v}")

    # Save schema
    with open("schema_output.json", "w") as f:
        json.dump(schema, f, indent=2)
    print("\nSchema written to schema_output.json")

    # Flatten sample correctly
    flat_sample = flatten_record(records[0])
    print("\n# Sample Flattened Row (sanity check):")
    print(json.dumps(flat_sample, indent=2))

    # Build DQ suggestions
    dq_suggestions = []
    for col_name, col_type in schema.items():
        clean = col_name.replace(".", "_")
        if "id" in col_name.lower():
            dq_suggestions.append(f"- `{clean}` should not be null")
        if "ts" in col_name.lower() or "date" in col_name.lower():
            dq_suggestions.append(f"- `{clean}` should be a valid timestamp")
        if col_type in ("int", "double"):
            dq_suggestions.append(f"- `{clean}` should not be negative")

    print("\n# Suggested Data Quality Checks:")
    for d in dq_suggestions:
        print(d)

    # Generate ETL code
    generated_code = generate_pyspark_code(schema)
    print("\n# Generated PySpark ETL Code:")
    print(generated_code)

    # FINAL: Generate LLM Prompt
    llm_prompt = generate_llm_prompt(schema, flat_sample, dq_suggestions, generated_code)
    with open("llm_prompt.txt", "w") as f:
        f.write(llm_prompt)
    print("\nllm_prompt.txt created successfully")


if __name__ == "__main__":
    main()
