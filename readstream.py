from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, to_timestamp, from_unixtime, lit
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType, IntegerType

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

schema = StructType().add("id_transaction", StringType()) \
                     .add("type_transaction", StringType()) \
                     .add("montant", FloatType()) \
                     .add("devise", StringType()) \
                     .add("date", TimestampType()) \
                     .add("lieu", StringType()) \
                     .add("moyen_paiement", StringType()) \
                     .add("details", StructType().add("produit", StringType()) \
                                                .add("quantite", IntegerType()) \
                                                .add("prix_unitaire", FloatType())) \
                     .add("utilisateur", StructType().add("id_utilisateur", StringType()) \
                                                     .add("nom", StringType()) \
                                                     .add("adresse", StringType()) \
                                                     .add("email", StringType())) \
                     .add("timezone", StringType())

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "ne_topic") \
    .load()
    


#conversion de la devise
df = df.withColumn("devise", when(col("devise") == "USD", "EUR").otherwise(col("devise")))
df = df.withColumn("montant", col("montant") * 0.93)


# charger les données depuis kafka
df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")


# Conversion de la date
df = df.withColumn("date", from_unixtime(col("date").cast("double"), "EEEE dd MMMM yyyy").alias("date"))


# suppression des lignes de la colonne "lieu" sont à "None"
df = df.filter(col("lieu").contains("None"))

# Filter les lignes qui ont "moyen_paiement" = "erreur"
df = df.filter(col("moyen_paiement") != "erreur")


# Add a new column "timezone" based on "lieu"
df = df.withColumn("timezone", when(col("lieu") == "Paris", "Europe/Paris").otherwise("UTC"))

# écrire le stream sous format parquet
query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "metadata") \
    .option("path", "elem") \
    .option("queryName", "myQuery") \
    .start()

query.awaitTermination()
