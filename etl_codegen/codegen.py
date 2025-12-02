from typing import Dict

def generate_pyspark_code(schema: Dict[str, str],
                          input_path: str = "s3://your-bucket/input/",
                          output_path: str = "s3://your-bucket/output/") -> str:
    """
    schema: {column_name: spark_type}
    """
    lines = []
    lines.append("from pyspark.sql import SparkSession")
    lines.append("from pyspark.sql.functions import col")
    lines.append("")
    lines.append("spark = SparkSession.builder.appName('etl_codegen').getOrCreate()")
    lines.append("")
    lines.append(f'df_raw = spark.read.json("{input_path}")')
    lines.append("")
    lines.append("# Select & cast columns")
    select_exprs = []
    for col_name, col_type in schema.items():
        if col_type == "string":
            select_exprs.append(f'col("{col_name}").alias("{col_name.replace(".", "_")}")')
        else:
            select_exprs.append(
                f'col("{col_name}").cast("{col_type}").alias("{col_name.replace(".", "_")}")'
            )

        lines.append("df = df_raw.select(")
    for i, expr in enumerate(select_exprs):
        comma = "," if i < len(select_exprs) - 1 else ""
        lines.append(f"    {expr}{comma}")
    lines.append(")")

   
    lines.append("")
    lines.append('# Write curated data')
    lines.append('df.write.mode("overwrite").parquet("<output_path>")')
    lines.append("")
    lines.append("spark.stop()")