import boto3
import json

def refine_etl_with_bedrock(prompt):
    bedrock = boto3.client(
        service_name="bedrock-runtime",
        region_name="us-east-1"
    )

    model_id = "meta.llama3-70b-instruct-v1:0"   # DOES NOT REQUIRE APPROVAL 🎉

    body = {
        "prompt": prompt,
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
    refined_text = output["generation"]

    with open("refined_etl.py", "w") as f:
        f.write(refined_text)

    return refined_text