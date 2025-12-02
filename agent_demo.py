import json
from etl_codegen.schema_infer import infer_schema_from_samples, flatten_record
from etl_codegen.codegen import generate_pyspark_code
from refine_etl_bedrock import refine_etl_with_bedrock   # YOU WILL CREATE THIS FILE BELOW


def simulate_agent(json_path="examples/sample_payload.json"):
    print("AGENT WORKFLOW — AI-ASSISTED ETL GENERATION\n")

    # Step 1 — Load JSON
    print("🔹 Step 1 — Read JSON Input\n")
    with open(json_path) as f:
        record = json.load(f)
    print(json.dumps(record, indent=2))

    # Step 2 — Infer schema
    print("\n🔹 Step 2 — Infer Schema\n")
    schema = infer_schema_from_samples([record])
    print(json.dumps(schema, indent=2))

    # Step 3 — Generate draft ETL
    print("\n🔹 Step 3 — Generate Draft PySpark ETL\n")
    draft_etl = generate_pyspark_code(schema)
    print(draft_etl)

    # Step 4 — Flatten sample record
    print("\n🔹 Step 4 — Flatten Sample Record\n")
    flat = flatten_record(record)
    print(json.dumps(flat, indent=2))

    # Step 5 — Prepare LLM Prompt
    print("\n🔹 Step 5 — Generate LLM Prompt\n")
    from main import generate_llm_prompt
    prompt = generate_llm_prompt(schema, flat, [], draft_etl)

    # Write prompt for debugging
    with open("llm_prompt.txt", "w") as f:
        f.write(prompt)

    # Step 6 — Ask Bedrock to refine ETL
    print("\n🔹 Step 6 — Refining ETL via Amazon Bedrock\n")
    print("Using Bedrock model: meta.llama3-70b-instruct-v1:0")
    refined_etl = refine_etl_with_bedrock(prompt)

    print("\n✨ FINAL — Refined ETL (from Bedrock):\n")
    print(refined_etl)


if __name__ == "__main__":
    simulate_agent()