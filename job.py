import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf

import pyarrow as pa

spark = (
    SparkSession.builder
    .appName("PySpark to PyArrow GDS Example")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.execution.arrow.pyspark.maxRecordsPerBatch", 10_000)
    #.config("viewsEnabled", "true")
    #.config("materializationDataset", "<dataset>")
    .getOrCreate()
)

df = (spark.read
      .format("bigquery")
      .load("bigquery-public-data.samples.wikipedia")
      .select("title").limit(10_000)
      .repartition(10))
print(f">>> Starting with {df.rdd.getNumPartitions()}")

def batch(iterator):
    data = dict()
    for row in iterator:
        for field in row.asDict():
            data.get(field, []).append(row[field])

    keys = data.keys()
    arrays = [pa.array(data[k]) for k in keys]
    batch = pa.record_batch(arrays, names=keys)
    yield batch.serialize().to_pybytes()

results = df.rdd.mapPartitions(batch).collect()
print(results)

first = results[0]
