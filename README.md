# AI-Assisted PySpark ETL Code Generator (Agent-Ready)

This project is an **data engineering tool** that:
- infers schema from nested JSON,
- flattens sample data,
- generates draft PySpark ETL code,
- produces suggested data quality checks,
- writes structured metadata outputs,
- and creates a fully structured **LLM-ready prompt** for further ETL refinement.

It demonstrates how  prompt engineering can accelerate ETL development and onboarding new datasets.

---

It is explicitly designed to be used by:
- **AI Coding Assistants** (ChatGPT, Amazon Q, Cursor, Copilot), and  
- **AI Agents / Agentic Workflows** (Bedrock Agents, LangChain, Step Functions Agents).

---

#  What This Tool Does

Given a sample JSON, It produces:

### ✔ 1. Inferred flattened schema  
### ✔ 2. Draft PySpark ETL code  
### ✔ 3. Sample flattened record  
### ✔ 4. Suggested Data Quality (DQ) checks  
### ✔ 5. schema_output.json  
### ✔ 6. LLM-Ready Prompt (llm_prompt.txt)

The LLM prompt contains everything an AI agent needs to refine the ETL into a production-ready script.

## 🏗 Architecture (AI-First Engineering)

```mermaid
flowchart LR
    A[Sample JSON Input] --> B[Schema Infer Engine]
    B --> C[Flatten Sample Generator]
    C --> D[Draft PySpark ETL Generator]
    D --> E[LLM Prompt Generator]
    E --> F[LLM or AI Agent<br/>ChatGPT / Amazon Q / Bedrock]
    F --> G[Refined, Production ETL Script]

    subgraph Optional AWS Deployment
        B --> L1[Lambda]
        C --> L2[Lambda]
        D --> L3[Lambda or Fargate]
        E --> L4[Lambda]
    end
```
