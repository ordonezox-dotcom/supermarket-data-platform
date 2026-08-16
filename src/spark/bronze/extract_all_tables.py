import os

from pyspark.sql import SparkSession


DB_HOST = os.getenv("SPARK_SOURCE_DB_HOST")
DB_PORT = os.getenv("SPARK_SOURCE_DB_PORT")
DB_NAME = os.getenv("SPARK_SOURCE_DB_NAME")
DB_USER = os.getenv("SPARK_SOURCE_DB_USER")
DB_PASSWORD = os.getenv("SPARK_SOURCE_DB_PASSWORD")


SOURCE_TABLES = [
    "clientes",
    "productos",
    "sucursales",
    "vendedores",
    "inventario",
    "facturas",
    "detalles_factura",
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("bronze_full_extraction")
        .getOrCreate()
    )


def extract_table(
    spark: SparkSession,
    table_name: str,
) -> None:

    jdbc_url = (
        f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/"
        f"{DB_NAME}"
    )

    connection_properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    print(f"\nExtrayendo tabla: {table_name}")

    df = spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=connection_properties,
    )

    row_count = df.count()

    print(
        f"Registros encontrados en "
        f"{table_name}: {row_count}"
    )

    bronze_path = (
        f"/opt/spark-data/bronze/{table_name}"
    )

    (
        df.write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    print(
        f"Tabla {table_name} guardada "
        f"correctamente en Bronze."
    )


def extract_all_tables():

    spark = create_spark_session()

    try:

        for table_name in SOURCE_TABLES:

            extract_table(
                spark=spark,
                table_name=table_name,
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    extract_all_tables()