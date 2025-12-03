import boto3
import json
import re

def clean_llm_output(text: str) -> str:
    """
    Remove markdown fences but DO NOT aggressively strip content,
    because Llama may not include the phrase 'PySpark ETL script'.
    """
    # Remove common code fences
    text = text.replace("```python", "")
    text = text.replace("```", "")
    return text.strip()


def refine_etl_with_bedrock(prompt):
    print("\n[Bedrock] Calling Llama3...\n")

    bedrock = boto3.client(
        service_name="bedrock-runtime",
        region_name="us-east-1"
    )

    model_id = "meta.llama3-70b-instruct-v1:0"

    strict_prompt = (
        prompt
        + "\n\nReturn ONLY the PySpark code. "
        + "No markdown, no backticks, no explanations."
    )

    body = {
        "prompt": strict_prompt,
        "max_gen_len": 4096,
        "temperature": 0.2,
        "top_p": 0.9
    }

    response = bedrock.invoke_model(
        modelId=model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(body)
    )

    output = json.loads(response["body"].read())

    print("\n--- RAW BEDROCK RESPONSE ---\n")
    print(json.dumps(output, indent=2))

    raw_text = output.get("generation", "")

    if not raw_text:
        print("\n❗ Llama3 returned EMPTY generation.")
        print("❗ The prompt may be too long or structured incorrectly.")
        print("❗ Try simplifying the prompt or reducing sections.\n")

    clean_text = clean_llm_output(raw_text)

    if not clean_text:
        print("\n❗ CLEANED TEXT IS EMPTY.")
        print("❗ Clean-up removed everything (likely due to regex).")
        print("❗ Keeping RAW text instead.\n")
        clean_text = raw_text

    with open("refined_etl.py", "w") as f:
        f.write(clean_text)

    print("\n[Bedrock] refined_etl.py saved.\n")
    return clean_text