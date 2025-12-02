# AI-Assisted PySpark ETL Code Generator

This project is a lightweight backend “agent tool” that takes a sample JSON payload, 
**infers a flattened schema**, and **generates a ready-to-run PySpark ETL script**.  
It demonstrates how AI-powered or agentic systems can accelerate data engineering workflows by automating repetitive tasks like JSON flattening, schema inference, and ETL scaffolding.

## 🌟 What this tool does

✔ Accepts a sample JSON file  
✔ Automatically infers a flattened schema (with dot notation for nested fields)  
✔ Maps values to Spark-compatible data types  
✔ Generates a complete PySpark ETL script:
   - `spark.read.json(...)`
   - Column extraction & type casting
   - Output to Parquet
✔ Prints both the inferred schema and the generated code to the console

This backend can be used **standalone** or plugged into an **LLM/agent** as a callable tool.

---