"""
Job Spark : lit la couche bronze (les CSV bruts), nettoie et type les
donnees, retire les doublons, puis ecrit le resultat en couche silver
au format Parquet, partitionne par pickup_community_area.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark_session():
    """
    Cree la session Spark et configure la connexion au stockage objet
    MinIO/Silo via le connecteur s3a.
    """
    return (
        SparkSession.builder
        .appName("bronze_to_silver")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .getOrCreate()
    )


def clean_trips(df):
    """
    Applique les regles de nettoyage sur les trajets bruts : conversion
    des timestamps et des montants vers leurs vrais types, suppression
    des doublons sur trip_id, et filtrage des lignes clairement invalides
    (date de depart manquante, montant negatif).
    """
    return (
        df
        .withColumn("trip_start_timestamp", F.to_timestamp("trip_start_timestamp"))
        .withColumn("trip_end_timestamp", F.to_timestamp("trip_end_timestamp"))
        .withColumn("trip_total", F.col("trip_total").cast("double"))
        .withColumn("trip_miles", F.col("trip_miles").cast("double"))
        .withColumn("trip_seconds", F.col("trip_seconds").cast("int"))
        .dropDuplicates(["trip_id"])
        .filter(F.col("trip_start_timestamp").isNotNull())
        .filter(F.col("trip_total") >= 0)
    )


def main():

    spark = build_spark_session()

    df = spark.read.option("header", True).csv("s3a://datalake/bronze/taxi_trips/")
    df_clean = clean_trips(df)

    (
        df_clean.write
        .mode("overwrite")
        .partitionBy("pickup_community_area")
        .parquet("s3a://datalake/silver/taxi_trips/")
    )

    print(f"[bronze_to_silver] lignes ecrites en silver : {df_clean.count()}")
    spark.stop()


if __name__ == "__main__":
    main()