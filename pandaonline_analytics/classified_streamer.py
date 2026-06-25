.option('checkpointLocation', 'chkpoint')from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Session spark : le streamer qui lit les infos
spark = (SparkSession
    .builder
    .appName("Classified Streamer")
    .getOrCreate())

# On crée le data frame à partir du topic redpanda déjà crée sur l'instance du conteneur redpanda-1, port 9092
# Ce dataframe soouscrit lit les infos en continu qu'il y a sur ce topic "classified", comme cela est marqué
df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda-1:9092")
    .option("subscribe", "classifieds")
    .load())

#On créer un schéma JSON pour la transformation de donnée qui va être réalisé à la ligne suivante
jsonschema = StructType([
    StructField("classifiedId", IntegerType()),
    StructField("userId", IntegerType()),
    StructField("url", StringType())
])

#On applique le schéma JSON crée précedemment pour tranformer vérifitable les données et obtenir un nouveau df
df = (df
    .select(from_json(col("value").cast(StringType()), jsonschema).alias("value"))
    .select("value.*")
    .withColumn('validVisit', when(col('classifiedId') > 2000, "Yes").otherwise("No"))
    .withColumn("value", to_json(struct(col("userId"), col("validVisit")))))

#On écrit le nouveau df avec les données transformées dans un nouveau topic (visit-validity) grâce à SPARK
#En effet, ce sont les libraires spark qui nous permettent d'avoir un df un peu particulier (voir déclaration df)
query = (df
    .writeStream
    .format('kafka')
    .option('kafka.bootstrap.servers', 'redpanda-1:9092')
    .option('topic', 'visit-validity')
    .option('checkpointLocation', '/home/muser/chkpoint')
    .start())

query.awaitTermination()
