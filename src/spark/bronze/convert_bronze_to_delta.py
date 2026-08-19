from delta.tables import DeltaTable
from pyspark.sql import SparkSession


BRONZE_TABLES = [
    "facturas",
    "detalles_factura",
    "clientes",
    "productos",
    "inventario",
    "sucursales",
    "vendedores",
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("convert_bronze_to_delta")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def convert_table_to_delta(
    spark,
    table_name,
):
    bronze_path = (
        f"/opt/spark-data/bronze/{table_name}"
    )

    print(
        f"\n[{table_name}] "
        f"Revisando tabla Bronze..."
    )

    # Evita intentar convertir nuevamente
    # una tabla que ya sea Delta.
    if DeltaTable.isDeltaTable(
        spark,
        bronze_path,
    ):
        print(
            f"[{table_name}] "
            f"Ya está en formato Delta."
        )
        return

    print(
        f"[{table_name}] "
        f"Convirtiendo Parquet -> Delta..."
    )

    DeltaTable.convertToDelta(
        spark,
        f"parquet.`{bronze_path}`",
    )

    print(
        f"[{table_name}] "
        f"Conversión completada."
    )


def convert_all_tables():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    try:

        for table_name in BRONZE_TABLES:

            convert_table_to_delta(
                spark=spark,
                table_name=table_name,
            )

    finally:

        spark.stop()


if __name__ == "__main__":
    convert_all_tables()