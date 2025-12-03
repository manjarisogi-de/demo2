from typing import Dict

def generate_pyspark_code(schema: Dict[str, str],
                          input_path: str = "<input_path>",
                          output_path: str = "<output_path>") -> str:

    lines = []
    lines.append("from pyspark.sql import SparkSession")
    lines.append("from pyspark.sql.functions import col")
    lines.append("")
    lines.append("spark = SparkSession.builder.appName('etl_codegen').getOrCreate()")
    lines.append("")
    lines.append(f'df_raw = spark.read.json("{input_path}")')
    lines.append("")

    # Build select expression
    lines.append("df = df_raw.select(")
    select_exprs = []

    for col_name, col_type in schema.items():
        alias = col_name.replace(".", "_")
        if col_type in ("int", "double"):
            select_exprs.append(f'    col("{col_name}").cast("{col_type}").alias("{alias}")')
        else:
            select_exprs.append(f'    col("{col_name}").alias("{alias}")')

    for i, expr in enumerate(select_exprs):
        comma = "," if i < len(select_exprs) - 1 else ""
        lines.append(expr + comma)

    lines.append(")")
    lines.append("")

    lines.append(f'df.write.mode("overwrite").parquet("{output_path}")')
    lines.append("")
    lines.append("spark.stop()")

    return "\n".join(lines)