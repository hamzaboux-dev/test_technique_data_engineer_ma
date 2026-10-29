"""
Spark job : lit la couche silver et calcule les agregats metier (KPIs)
attendus par la BI, ecrits en Parquet dans la couche gold.

KPIs :
  - revenus par jour
  - duree moyenne des trajets (minutes)
  - top zones de prise en charge
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("silver_to_gold")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .getOrCreate()
    )

    silver = spark.read.parquet("s3a://datalake/silver/taxi_trips/")

    # KPI 1 : revenus par jour
    revenue_by_day = (
        silver
        .withColumn("trip_date", F.to_date("trip_start_timestamp"))
        .groupBy("trip_date")
        .agg(
            F.sum("trip_total").alias("total_revenue"),
            F.count("*").alias("nb_trips"),
        )
        .orderBy("trip_date")
    )

    # KPI 2 : duree moyenne des trajets (en minutes)
    avg_duration = silver.agg(
        F.avg(F.col("trip_seconds") / 60).alias("avg_duration_minutes")
    )

    # KPI 3 : top zones de prise en charge
    top_pickup_zones = (
        silver
        .groupBy("pickup_community_area")
        .agg(F.count("*").alias("nb_trips"))
        .orderBy(F.desc("nb_trips"))
        .limit(20)
    )

    revenue_by_day.write.mode("overwrite").parquet("s3a://datalake/gold/revenue_by_day/")
    avg_duration.write.mode("overwrite").parquet("s3a://datalake/gold/avg_duration/")
    top_pickup_zones.write.mode("overwrite").parquet("s3a://datalake/gold/top_pickup_zones/")

    print("[silver_to_gold] gold ecrit : revenue_by_day, avg_duration, top_pickup_zones")
    spark.stop()


if __name__ == "__main__":
    main()
