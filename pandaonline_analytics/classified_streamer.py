from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (SparkSession
    .builder
    .appName("Classified Streamer")
    .getOrCreate())

df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda-1:9092")
    .option("subscribe", "classifieds")
    .load())

jsonschema = StructType([
    StructField("classifiedId", IntegerType()),
    StructField("userId", IntegerType()),
    StructField("url", StringType())
])

df = (df
    .select(from_json(col("value").cast(StringType()), jsonschema).alias("value"))
    .select("value.*")
    .withColumn('validVisit', when(col('classifiedId') > 2000, "Yes").otherwise("No"))
    .withColumn("value", to_json(struct(col("userId"), col("validVisit")))))

query = (df
    .writeStream
    .format('kafka')
    .option('kafka.bootstrap.servers', 'redpanda-1:9092')
    .option('topic', 'visit-validity')
    .option('checkpointLocation', '/home/muser/chkpoint')
    .start())

query.awaitTermination()
