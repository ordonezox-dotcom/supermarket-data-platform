import os

from pyspark.sql import SparkSession


# Variables de entorno
DB_HOST = os.getenv("SPARK_SOURCE_DB_HOST")
DB_PORT = os.getenv("SPARK_SOURCE_DB_PORT")
DB_NAME = os.getenv("SPARK_SOURCE_DB_NAME")
DB_USER = os.getenv("SPARK_SOURCE_DB_USER")
DB_PASSWORD = os.getenv("SPARK_SOURCE_DB_PASSWORD")


def create_spark_session():
    return (
        SparkSession.builder
        .appName("bronze_facturas")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )


def extract_facturas():

    spark = create_spark_session()

    jdbc_url = (
        f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/"
        f"{DB_NAME}"
    )

    connection_properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    print("Leyendo tabla facturas desde PostgreSQL...")

    df_facturas = spark.read.jdbc(
        url=jdbc_url,
        table="facturas",
        properties=connection_properties,
    )

    print("Esquema de facturas:")
    df_facturas.printSchema()

    print("Cantidad de registros:")
    print(df_facturas.count())

    print("Primeras filas:")
    df_facturas.show(10, truncate=False)

    bronze_path = "/opt/spark-data/bronze/facturas"

    print(f"Guardando facturas en Bronze: {bronze_path}")

    (
    df_facturas.write
    .mode("overwrite")
    .parquet(bronze_path)
    )

    print("Facturas guardadas correctamente en Bronze.")

    spark.stop()


if __name__ == "__main__":
    extract_facturas()