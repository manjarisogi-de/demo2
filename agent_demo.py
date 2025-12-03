import json
from etl_codegen.schema_infer import infer_schema_from_samples, flatten_record
from etl_codegen.codegen import generate_pyspark_code
from refine_etl_bedrock import refine_etl_with_bedrock


def simulate_agent(json_path="examples/sample_payload.json"):
    print("AGENT WORKFLOW — AI-ASSISTED ETL GENERATION\n")

    with open(json_path) as f:
        record = json.load(f)

    print("🔹 Step 1 — Input JSON\n", json.dumps(record, indent=2))

    schema = infer_schema_from_samples([record])
    print("\n🔹 Step 2 — Schema\n", json.dumps(schema, indent=2))

    draft_etl = generate_pyspark_code(schema)
    print("\n🔹 Step 3 — Draft ETL\n", draft_etl)

    flat = flatten_record(record)
    print("\n🔹 Step 4 — Flattened Sample\n", json.dumps(flat, indent=2))

    array_cols = [col for col, t in schema.items() if t == "array_of_struct"]
    print("\n🔹 Step 5 — Array Columns:", array_cols)

    from main import generate_llm_prompt
    prompt = generate_llm_prompt(schema, flat, [], draft_etl, array_cols)


    with open("llm_prompt.txt", "w") as f:
        f.write(prompt)

    print("\n🔹 Step 6 — Refining ETL via Bedrock (Llama3)\n")
    refined_etl = refine_etl_with_bedrock(prompt)

    print("\n✨ FINAL — Refined ETL\n")
    print(refined_etl)


if __name__ == "__main__":
    simulate_agent()