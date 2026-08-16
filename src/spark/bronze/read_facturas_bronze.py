from pyspark.sql import SparkSession


def create_spark_session():
    return (
        SparkSession.builder
        .appName("read_bronze_facturas")
        .getOrCreate()
    )


def read_bronze_facturas():
    spark = create_spark_session()

    bronze_path = "/opt/spark-data/bronze/facturas"

    print("Leyendo facturas directamente desde Bronze...")

    df_facturas = spark.read.parquet(bronze_path)

    print("Esquema:")
    df_facturas.printSchema()

    print("Cantidad de facturas:")
    print(df_facturas.count())

    print("Primeras 10 facturas:")
    df_facturas.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    read_bronze_facturas()