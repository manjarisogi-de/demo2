import json
import sys
from etl_codegen.schema_infer import infer_schema_from_samples
from etl_codegen.codegen import generate_pyspark_code
from etl_codegen.schema_infer import flatten_record  # import the flatten helper


def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py sample.json")
        sys.exit(1)

    sample_path = sys.argv[1]
    with open(sample_path, "r") as f:
        # support either a single JSON object or a list
        content = f.read().strip()
        if content.startswith("["):
            records = json.loads(content)
        else:
            records = [json.loads(content)]

    schema = infer_schema_from_samples(records)
    flat_sample = flatten_record(records[0])
    print("\n# Sample Flattened Row:")
    print(json.dumps(flat_sample, indent=2))
    print("# Inferred schema:")
    for k, v in schema.items():
        print(f"#   {k}: {v}")

    print("")
    print("# Generated PySpark ETL code:")
    code = generate_pyspark_code(schema)
    print(code)
if __name__ == "__main__":
    main()
