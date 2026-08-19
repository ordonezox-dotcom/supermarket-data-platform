import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURACIÓN DE POSTGRESQL
# ============================================================

DB_HOST = os.getenv("SPARK_SOURCE_DB_HOST")
DB_PORT = os.getenv("SPARK_SOURCE_DB_PORT")
DB_NAME = os.getenv("SPARK_SOURCE_DB_NAME")
DB_USER = os.getenv("SPARK_SOURCE_DB_USER")
DB_PASSWORD = os.getenv("SPARK_SOURCE_DB_PASSWORD")


# ============================================================
# CONFIGURACIÓN DE ESTRATEGIAS POR TABLA
# ============================================================

TABLE_CONFIG = {
    "facturas": {
        "strategy": "incremental_id",
        "cursor_column": "factura_id",
    },

    "detalles_factura": {
        "strategy": "incremental_id",
        "cursor_column": "detalle_id",
    },

    "clientes": {
        "strategy": "incremental_timestamp",
        "cursor_column": "updated_at",
    },

    "productos": {
        "strategy": "incremental_timestamp",
        "cursor_column": "updated_at",
    },

    "inventario": {
        "strategy": "incremental_timestamp",
        "cursor_column": "fecha_actualizacion",
    },

    "sucursales": {
        "strategy": "full_reload",
    },

    "vendedores": {
        "strategy": "full_reload",
    },
}


# ============================================================
# CREACIÓN DE SPARK SESSION CON DELTA LAKE
# ============================================================

def create_spark_session():
    return (
        SparkSession.builder
        .appName("bronze_delta_extraction")
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


# ============================================================
# CONFIGURACIÓN JDBC
# ============================================================

def get_jdbc_url():
    return (
        f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/"
        f"{DB_NAME}"
    )


def get_connection_properties():
    return {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver",
    }


# ============================================================
# CARGA INICIAL
# ============================================================

def full_initial_load(
    spark,
    table_name,
    bronze_path,
):
    print(
        f"[{table_name}] Primera ejecución: "
        f"carga completa en Delta."
    )

    df = spark.read.jdbc(
        url=get_jdbc_url(),
        table=table_name,
        properties=get_connection_properties(),
    )

    df.persist()

    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(bronze_path)
    )

    df.unpersist()

    print(
        f"[{table_name}] "
        f"{row_count} registros cargados en Delta."
    )


# ============================================================
# FULL RELOAD
# ============================================================

def full_reload(
    spark,
    table_name,
    bronze_path,
):
    print(
        f"[{table_name}] Ejecutando full reload."
    )

    df = spark.read.jdbc(
        url=get_jdbc_url(),
        table=table_name,
        properties=get_connection_properties(),
    )

    df.persist()

    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(bronze_path)
    )

    df.unpersist()

    print(
        f"[{table_name}] "
        f"{row_count} registros recargados."
    )


# ============================================================
# INCREMENTAL POR ID
# ============================================================

def incremental_by_id(
    spark,
    table_name,
    cursor_column,
    bronze_path,
):
    df_bronze = (
        spark.read
        .format("delta")
        .load(bronze_path)
    )

    last_id = (
        df_bronze
        .agg(F.max(cursor_column))
        .collect()[0][0]
    )

    print(
        f"[{table_name}] Último "
        f"{cursor_column}: {last_id}"
    )

    query = f"""
    (
        SELECT *
        FROM {table_name}
        WHERE {cursor_column} > {last_id}
    ) AS incremental_data
    """

    df_new = spark.read.jdbc(
        url=get_jdbc_url(),
        table=query,
        properties=get_connection_properties(),
    )

    df_new.persist()

    new_count = df_new.count()

    if new_count == 0:
        print(
            f"[{table_name}] "
            f"No hay registros nuevos."
        )

        df_new.unpersist()
        return

    (
        df_new.write
        .format("delta")
        .mode("append")
        .save(bronze_path)
    )

    df_new.unpersist()

    print(
        f"[{table_name}] "
        f"{new_count} registros nuevos agregados."
    )


# ============================================================
# INCREMENTAL POR TIMESTAMP
# ============================================================

def incremental_by_timestamp(
    spark,
    table_name,
    cursor_column,
    bronze_path,
):
    df_bronze = (
        spark.read
        .format("delta")
        .load(bronze_path)
    )

    last_timestamp = (
        df_bronze
        .agg(F.max(cursor_column))
        .collect()[0][0]
    )

    print(
        f"[{table_name}] Último "
        f"{cursor_column}: "
        f"{last_timestamp}"
    )

    timestamp_string = (
        last_timestamp.strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
    )

    query = f"""
    (
        SELECT *
        FROM {table_name}
        WHERE {cursor_column}
              > TIMESTAMP '{timestamp_string}'
    ) AS incremental_data
    """

    df_changes = spark.read.jdbc(
        url=get_jdbc_url(),
        table=query,
        properties=get_connection_properties(),
    )

    df_changes.persist()

    change_count = df_changes.count()

    if change_count == 0:
        print(
            f"[{table_name}] "
            f"No hay registros nuevos "
            f"ni modificados."
        )

        df_changes.unpersist()
        return

    (
        df_changes.write
        .format("delta")
        .mode("append")
        .save(bronze_path)
    )

    df_changes.unpersist()

    print(
        f"[{table_name}] "
        f"{change_count} registros "
        f"nuevos/modificados agregados."
    )


# ============================================================
# EXTRACCIÓN DE UNA TABLA
# ============================================================

def extract_table(
    spark,
    table_name,
    config,
):
    bronze_path = (
        f"/opt/spark-data/bronze/"
        f"{table_name}"
    )

    strategy = config["strategy"]

    # --------------------------------------------------------
    # FULL RELOAD
    # --------------------------------------------------------

    if strategy == "full_reload":

        # Si todavía no existe la tabla Delta,
        # hacemos una carga inicial.
        if not DeltaTable.isDeltaTable(
            spark,
            bronze_path,
        ):
            full_initial_load(
                spark=spark,
                table_name=table_name,
                bronze_path=bronze_path,
            )

            return

        full_reload(
            spark=spark,
            table_name=table_name,
            bronze_path=bronze_path,
        )

        return

    # --------------------------------------------------------
    # TABLAS INCREMENTALES
    # --------------------------------------------------------

    if not Path(bronze_path).exists():

        full_initial_load(
            spark=spark,
            table_name=table_name,
            bronze_path=bronze_path,
        )

        return

    # Existe la carpeta, pero no es Delta.
    # Evitamos mezclar Parquet puro con Delta accidentalmente.
    if not DeltaTable.isDeltaTable(
        spark,
        bronze_path,
    ):
        raise RuntimeError(
            f"[{table_name}] La carpeta Bronze existe "
            f"pero todavía no es una tabla Delta. "
            f"Ejecuta primero "
            f"convert_bronze_to_delta.py."
        )

    cursor_column = config["cursor_column"]

    if strategy == "incremental_id":

        incremental_by_id(
            spark=spark,
            table_name=table_name,
            cursor_column=cursor_column,
            bronze_path=bronze_path,
        )

    elif strategy == "incremental_timestamp":

        incremental_by_timestamp(
            spark=spark,
            table_name=table_name,
            cursor_column=cursor_column,
            bronze_path=bronze_path,
        )

    else:
        raise ValueError(
            f"Estrategia desconocida "
            f"para {table_name}: "
            f"{strategy}"
        )


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

def extract_all_tables():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    try:
        for table_name, config in TABLE_CONFIG.items():

            print(
                "\n"
                "===================================="
            )

            print(
                f"Procesando tabla: {table_name}"
            )

            print(
                f"Estrategia: {config['strategy']}"
            )

            print(
                "Formato Bronze: DELTA"
            )

            print(
                "===================================="
            )

            extract_table(
                spark=spark,
                table_name=table_name,
                config=config,
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    extract_all_tables()