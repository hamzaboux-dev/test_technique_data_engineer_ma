"""
Spark job : lit la couche bronze (CSV bruts), nettoie / type / deduplique,
et ecrit la couche silver en Parquet, partitionnee par pickup_community_area.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("bronze_to_silver")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .getOrCreate()
    )

    df = spark.read.option("header", True).csv("s3a://datalake/bronze/taxi_trips/")

    df_clean = (
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
