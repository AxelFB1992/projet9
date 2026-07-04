import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, count, window,
    to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

# ─────────────────────────────────────────
# 1. SparkSession
# ─────────────────────────────────────────
spark = (SparkSession.builder
    .appName("TicketProcessor")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "1g")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────
# 2. Schéma des tickets
# ─────────────────────────────────────────
schema = StructType([
    StructField("ticket_id",    StringType(),    True),
    StructField("client_id",    StringType(),    True),
    StructField("created_at",   StringType(),    True),
    StructField("demande",      StringType(),    True),
    StructField("type_demande", StringType(),    True),
    StructField("priorite",     StringType(),    True),
])

# ─────────────────────────────────────────
# 3. Lecture du stream Redpanda
# ─────────────────────────────────────────
raw_stream = (spark.readStream
    .format("kafka")
#    .option("kafka.bootstrap.servers", "localhost:19092")
    .option("kafka.bootstrap.servers", "172.26.199.199:19092")
    .option("subscribe", "client_tickets")
    .option("startingOffsets", "latest")
    #.option("startingOffsets", "earliest")  # lire depuis le début
    .option("failOnDataLoss", "false")  # résilience : ne pas planter si des offsets sont manquants
    .load())

# ─────────────────────────────────────────
# 4. Désérialisation JSON
# ─────────────────────────────────────────
tickets = (raw_stream
    .select(from_json(col("value").cast(StringType()), schema).alias("data"))
    .select("data.*")
    .withColumn("created_at", to_timestamp(col("created_at")))
    .withColumn("ingested_at", current_timestamp()))

# ─────────────────────────────────────────
# 5. Assignation de l'équipe de support
# ─────────────────────────────────────────
tickets_enrichis = tickets.withColumn(
    "equipe_support",
    when(col("type_demande") == "Incident technique",    "Équipe Infrastructure")
    .when(col("type_demande") == "Demande d'information","Équipe Support Niveau 1")
    .when(col("type_demande") == "Facturation",          "Équipe Comptabilité")
    .when(col("type_demande") == "Accès et permissions", "Équipe Sécurité")
    .when(col("type_demande") == "Performance",          "Équipe Performance")
    .otherwise("Équipe Support Général")
)

# ─────────────────────────────────────────
# 6. Agrégations en temps réel
# ─────────────────────────────────────────

# 6a. Tickets par priorité (fenêtre de 1 minute)
tickets_par_priorite = (tickets_enrichis
    .withWatermark("created_at", "30 seconds")
    .groupBy(
        window(col("created_at"), "1 minute"),
        col("priorite")
    )
    .agg(count("*").alias("nb_tickets")))

# 6b. Tickets par type de demande
tickets_par_type = (tickets_enrichis
    .withWatermark("created_at", "30 seconds")
    .groupBy(
        window(col("created_at"), "1 minute"),
        col("type_demande")
    )
    .agg(count("*").alias("nb_tickets")))

# 6c. Tickets par équipe de support
tickets_par_equipe = (tickets_enrichis
    .withWatermark("created_at", "30 seconds")
    .groupBy(
        window(col("created_at"), "1 minute"),
        col("equipe_support")
    )
    .agg(count("*").alias("nb_tickets")))

# ─────────────────────────────────────────
# 7. Écriture des résultats (console)
# ─────────────────────────────────────────
# Créer les dossiers de sortie
os.makedirs("output/tickets", exist_ok=True)
os.makedirs("output/agregations_priorite", exist_ok=True)

# Stream principal — tickets enrichis → Parquet
query_tickets = (tickets_enrichis
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", "output/tickets")
    .option("checkpointLocation", "/tmp/checkpoint_tickets")
    .trigger(processingTime="10 seconds")
    .start())

# Agrégation par priorité → Parquet
query_priorite = (tickets_par_priorite
    .writeStream
    .outputMode("append")  # "append" requis pour l'écriture fichier avec watermark
    .format("parquet")
    .option("path", "output/agregations_priorite")
    .option("checkpointLocation", "/tmp/checkpoint_priorite")
    .trigger(processingTime="30 seconds")
    .start())
    
# Stream principal — tickets enrichis -> CONSOLE
#query_tickets = (tickets_enrichis
#    .writeStream
#    .outputMode("append")
#    .format("console")
#    .option("truncate", "false")
#    .option("checkpointLocation", "/tmp/checkpoint_tickets")
#    .trigger(processingTime="5 seconds")
#    .start())

# Agrégation par priorité
#query_priorite = (tickets_par_priorite
#    .writeStream
#    .outputMode("update")
#    .format("console")
#    .option("truncate", "false")
#    .option("checkpointLocation", "/tmp/checkpoint_priorite")
#    .trigger(processingTime="10 seconds")
#    .start())

# Agrégation par type
#query_type = (tickets_par_type
#    .writeStream
#    .outputMode("update")
#    .format("console")
#    .option("truncate", "false")
#    .option("checkpointLocation", "/tmp/checkpoint_type")
#    .trigger(processingTime="10 seconds")
#    .start())

# Agrégation par équipe
#query_equipe = (tickets_par_equipe
#    .writeStream
#    .outputMode("update")
#    .format("console")
#    .option("truncate", "false")
#    .option("checkpointLocation", "/tmp/checkpoint_equipe")
#    .trigger(processingTime="10 seconds")
#    .start())

# ─────────────────────────────────────────
# 8. Attendre la terminaison
# ─────────────────────────────────────────
#spark.streams.awaitAnyTermination()
query_tickets.awaitTermination()
