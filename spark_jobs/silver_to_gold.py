"""
Job Spark : lit la couche silver et calcule les trois agregats metier
(KPIs) attendus par l'equipe BI, ecrits ensuite en Parquet dans la
couche gold.

KPIs produits :
  - revenus totaux par jour
  - duree moyenne des trajets, en minutes
  - top des zones de prise en charge les plus actives
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark_session():
    """
    Cree la session Spark et configure la connexion au stockage objet
    MinIO/Silo via le connecteur s3a .
    """
    return (
        SparkSession.builder
        .appName("silver_to_gold")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .getOrCreate()
    )


def compute_revenue_by_day(silver):
    """
    KPI 1 : additionne le revenu et le nombre de trajets par jour,
    a partir de la date de depart du trajet.
    """
    return (
        silver
        .withColumn("trip_date", F.to_date("trip_start_timestamp"))
        .groupBy("trip_date")
        .agg(
            F.sum("trip_total").alias("total_revenue"),
            F.count("*").alias("nb_trips"),
        )
        .orderBy("trip_date")
    )


def compute_avg_duration(silver):
    """
    KPI 2 : calcule la duree moyenne des trajets en minutes, a partir
    de trip_seconds converti.
    """
    return silver.agg(
        F.avg(F.col("trip_seconds") / 60).alias("avg_duration_minutes")
    )


def compute_top_pickup_zones(silver):
    """
    KPI 3 : classe les zones de prise en charge par nombre de trajets
    et ne garde que les 20 plus actives.
    """
    return (
        silver
        .groupBy("pickup_community_area")
        .agg(F.count("*").alias("nb_trips"))
        .orderBy(F.desc("nb_trips"))
        .limit(20)
    )


def main():

    spark = build_spark_session()
    silver = spark.read.parquet("s3a://datalake/silver/taxi_trips/")

    revenue_by_day = compute_revenue_by_day(silver)
    avg_duration = compute_avg_duration(silver)
    top_pickup_zones = compute_top_pickup_zones(silver)

    revenue_by_day.write.mode("overwrite").parquet("s3a://datalake/gold/revenue_by_day/")
    avg_duration.write.mode("overwrite").parquet("s3a://datalake/gold/avg_duration/")
    top_pickup_zones.write.mode("overwrite").parquet("s3a://datalake/gold/top_pickup_zones/")

    print("[silver_to_gold] gold ecrit : revenue_by_day, avg_duration, top_pickup_zones")
    spark.stop()


if __name__ == "__main__":
    main()