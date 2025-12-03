from pyspark.sql import functions as F

df = spark.read.json('input.json')

df = df.select(
    F.col('request_id'),
    F.col('user.id').alias('user_id'),
    F.col('user.segment').alias('user_segment'),
    F.col('event.type').alias('event_type'),
    F.to_timestamp(F.col('event.ts'), 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'').alias('event_ts'),
    F.col('amount').cast('double')
)

df = df.filter(
    F.col('request_id').isNotNull(),
    F.col('user_id').isNotNull(),
    F.col('event_type').isNotNull(),
    F.col('event_ts').isNotNull(),
    F.col('amount').isNotNull()
)

df.write.parquet('output.parquet', mode='overwrite', compression='snappy')