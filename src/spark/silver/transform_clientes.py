from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BRONZE_PATH = "/opt/spark-data/bronze/clientes"
SILVER_PATH = "/opt/spark-data/silver/clientes"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("silver_clientes_incremental")
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
# LIMPIEZA Y VALIDACIÓN
# ============================================================

def clean_clientes(df):

    df_valid = (
        df

        # Campos obligatorios
        .filter(
            F.col("cliente_id").isNotNull()
        )

        .filter(
            F.col("tipo_documento").isNotNull()
        )

        .filter(
            F.col("numero_documento").isNotNull()
        )

        .filter(
            F.col("nombre").isNotNull()
        )

        .filter(
            F.col("apellido").isNotNull()
        )

        .filter(
            F.col("updated_at").isNotNull()
        )
    )

    df_clean = (
        df_valid

        # Tipo documento uniforme
        .withColumn(
            "tipo_documento",
            F.upper(
                F.trim(
                    F.col("tipo_documento")
                )
            )
        )

        # Documento sin espacios
        .withColumn(
            "numero_documento",
            F.trim(
                F.col("numero_documento")
            )
        )

        # Nombres normalizados
        .withColumn(
            "nombre",
            F.initcap(
                F.trim(
                    F.col("nombre")
                )
            )
        )

        .withColumn(
            "apellido",
            F.initcap(
                F.trim(
                    F.col("apellido")
                )
            )
        )

        # Correo en minúsculas
        .withColumn(
            "correo",
            F.lower(
                F.trim(
                    F.col("correo")
                )
            )
        )

        # Teléfono limpio
        .withColumn(
            "telefono",
            F.trim(
                F.col("telefono")
            )
        )

        # Ciudad normalizada
        .withColumn(
            "ciudad",
            F.initcap(
                F.trim(
                    F.col("ciudad")
                )
            )
        )
    )

    df_quality = (
        df_clean

        # Documento no vacío
        .filter(
            F.length(
                F.col("numero_documento")
            ) > 0
        )

        # Nombre no vacío
        .filter(
            F.length(
                F.col("nombre")
            ) > 0
        )

        # Apellido no vacío
        .filter(
            F.length(
                F.col("apellido")
            ) > 0
        )

        # Correo básico válido si existe
        .filter(
            F.col("correo").isNull()
            |
            F.col("correo").rlike(
                r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            )
        )
    )

    # Elimina solamente filas técnicamente idénticas.
    # NO elimina versiones distintas del mismo cliente.
    df_final = (
        df_quality
        .dropDuplicates()
    )

    return df_final


# ============================================================
# PRIMERA CARGA
# ============================================================

def initial_load(
    spark,
):

    print(
        "\nSilver clientes no existe."
    )

    print(
        "Ejecutando carga inicial completa..."
    )

    df_bronze = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
    )

    bronze_count = df_bronze.count()

    print(
        f"Registros Bronze encontrados: "
        f"{bronze_count}"
    )

    df_silver = clean_clientes(
        df_bronze
    )

    silver_count = df_silver.count()

    print(
        f"Registros válidos para Silver: "
        f"{silver_count}"
    )

    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .save(SILVER_PATH)
    )

    print(
        "Carga inicial de clientes "
        "completada correctamente."
    )


# ============================================================
# CARGA INCREMENTAL
# ============================================================

def incremental_load(
    spark,
):

    print(
        "\nSilver clientes existente."
    )

    print(
        "Ejecutando transformación incremental..."
    )

    df_silver_existing = (
        spark.read
        .format("delta")
        .load(SILVER_PATH)
    )

    last_timestamp = (
        df_silver_existing
        .agg(
            F.max("updated_at")
        )
        .collect()[0][0]
    )

    print(
        f"Último updated_at procesado "
        f"en Silver: {last_timestamp}"
    )

    df_bronze = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
    )

    # Trae solamente versiones de Bronze
    # posteriores al último watermark de Silver.
    df_incremental = (
        df_bronze
        .filter(
            F.col("updated_at")
            > F.lit(last_timestamp)
        )
    )

    incremental_count = (
        df_incremental.count()
    )

    print(
        f"Registros nuevos/modificados "
        f"en Bronze: {incremental_count}"
    )

    if incremental_count == 0:

        print(
            "No hay datos nuevos "
            "para transformar."
        )

        return

    df_clean = clean_clientes(
        df_incremental
    )

    cleaned_count = df_clean.count()

    print(
        f"Registros después de limpieza: "
        f"{cleaned_count}"
    )

    if cleaned_count == 0:

        print(
            "Los registros encontrados "
            "no superaron las validaciones."
        )

        return

    # ========================================================
    # PROTECCIÓN CONTRA DUPLICADOS TÉCNICOS YA EXISTENTES
    # ========================================================

    comparison_columns = [
        "cliente_id",
        "tipo_documento",
        "numero_documento",
        "nombre",
        "apellido",
        "correo",
        "telefono",
        "ciudad",
        "fecha_nacimiento",
        "fecha_registro",
        "activo",
        "updated_at",
    ]

    df_to_insert = (
        df_clean.alias("new")
        .join(
            df_silver_existing.alias("old"),
            on=[
                F.col(
                    f"new.{column}"
                ).eqNullSafe(
                    F.col(
                        f"old.{column}"
                    )
                )
                for column
                in comparison_columns
            ],
            how="left_anti",
        )
    )

    insert_count = (
        df_to_insert.count()
    )

    print(
        f"Versiones realmente nuevas "
        f"para Silver: {insert_count}"
    )

    if insert_count == 0:

        print(
            "No hay nuevas versiones "
            "para agregar a Silver."
        )

        return

    (
        df_to_insert.write
        .format("delta")
        .mode("append")
        .save(SILVER_PATH)
    )

    print(
        f"{insert_count} nuevas versiones "
        f"agregadas correctamente a Silver."
    )


# ============================================================
# PIPELINE
# ============================================================

def transform_clientes():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:

        print(
            "\n===================================="
        )

        print(
            "BRONZE -> SILVER : CLIENTES"
        )

        print(
            "===================================="
        )

        if DeltaTable.isDeltaTable(
            spark,
            SILVER_PATH,
        ):

            incremental_load(
                spark
            )

        else:

            initial_load(
                spark
            )


        # ====================================================
        # MOSTRAR HISTORIAL DEL CLIENTE 1
        # ====================================================

        df_result = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
        )

        print(
            "\nVersiones del cliente 1 "
            "conservadas en Silver:"
        )

        (
            df_result
            .filter(
                F.col("cliente_id") == 1
            )
            .select(
                "cliente_id",
                "numero_documento",
                "nombre",
                "apellido",
                "ciudad",
                "updated_at",
            )
            .orderBy(
                F.col("updated_at")
            )
            .show(
                truncate=False
            )
        )

    finally:

        spark.stop()


if __name__ == "__main__":

    transform_clientes()