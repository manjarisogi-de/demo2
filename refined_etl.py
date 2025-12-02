```
from pyspark.sql.functions import col, explode, to_timestamp
from pyspark.sql.types import DoubleType

# Load the data
df = spark.read.json("path/to/data.json")

# Normalize timestamps
df = df.withColumn("event.ts", to_timestamp(col("event.ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

# Explode arrays (if applicable)
# None in this case

# Apply data quality checks
df = df.filter(col("request_id").isNotNull() & col("user.id").isNotNull() & col("event.type").isNotNull())
df = df.filter(col("amount").cast(DoubleType()).isNaN() == False)

# Final ETL script
df.write.parquet("path/to/output.parquet", mode="overwrite")
```
Note: The above code assumes that the input data is in a JSON file. You may need to adjust the `spark.read.json` line to match your actual data source.