from delta.tables import DeltaTable
from pyspark.sql import functions as F


def load_bronze_delta(
    spark,
    bronze_path: str,
):
    return (
        spark.read
        .format("delta")
        .load(bronze_path)
    )


def load_silver_delta(
    spark,
    silver_path: str,
):
    return (
        spark.read
        .format("delta")
        .load(silver_path)
    )


def get_last_cursor_value(
    df_silver,
    cursor_column: str,
):
    return (
        df_silver
        .agg(
            F.max(cursor_column)
        )
        .collect()[0][0]
    )


def get_incremental_dataframe(
    df_bronze,
    cursor_column: str,
    last_cursor_value,
):
    return (
        df_bronze
        .filter(
            F.col(cursor_column)
            > F.lit(last_cursor_value)
        )
    )


def remove_exact_duplicates(df):
    return df.dropDuplicates()


def write_initial_silver(
    df,
    silver_path: str,
):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .save(silver_path)
    )


def append_silver(
    df,
    silver_path: str,
):
    (
        df.write
        .format("delta")
        .mode("append")
        .save(silver_path)
    )


def remove_existing_exact_rows(
    df_new,
    df_silver,
):
    comparison_columns = df_new.columns

    return (
        df_new.alias("new")
        .join(
            df_silver.alias("old"),
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


def process_table(
    spark,
    table_name: str,
    cursor_column: str,
    transform_function,
):
    bronze_path = (
        f"/opt/spark-data/bronze/"
        f"{table_name}"
    )

    silver_path = (
        f"/opt/spark-data/silver/"
        f"{table_name}"
    )

    print(
        "\n===================================="
    )

    print(
        f"Procesando Silver: {table_name}"
    )

    print(
        f"Cursor: {cursor_column}"
    )

    print(
        "===================================="
    )

    df_bronze = load_bronze_delta(
        spark,
        bronze_path,
    )

    # ========================================================
    # PRIMERA CARGA
    # ========================================================

    if not DeltaTable.isDeltaTable(
        spark,
        silver_path,
    ):

        print(
            f"[{table_name}] "
            f"Silver no existe."
        )

        print(
            f"[{table_name}] "
            f"Ejecutando carga inicial completa."
        )

        bronze_count = (
            df_bronze.count()
        )

        df_clean = transform_function(
            df_bronze
        )

        df_clean = (
            remove_exact_duplicates(
                df_clean
            )
        )

        silver_count = (
            df_clean.count()
        )

        print(
            f"[{table_name}] "
            f"Bronze: {bronze_count}"
        )

        print(
            f"[{table_name}] "
            f"Silver limpio: {silver_count}"
        )

        write_initial_silver(
            df=df_clean,
            silver_path=silver_path,
        )

        print(
            f"[{table_name}] "
            f"Carga inicial completada."
        )

        return

    # ========================================================
    # CARGA INCREMENTAL
    # ========================================================

    print(
        f"[{table_name}] "
        f"Silver existente."
    )

    df_silver = load_silver_delta(
        spark,
        silver_path,
    )

    last_cursor_value = (
        get_last_cursor_value(
            df_silver,
            cursor_column,
        )
    )

    print(
        f"[{table_name}] "
        f"Último {cursor_column}: "
        f"{last_cursor_value}"
    )

    df_incremental = (
        get_incremental_dataframe(
            df_bronze=df_bronze,
            cursor_column=cursor_column,
            last_cursor_value=last_cursor_value,
        )
    )

    incremental_count = (
        df_incremental.count()
    )

    print(
        f"[{table_name}] "
        f"Registros incrementales: "
        f"{incremental_count}"
    )

    if incremental_count == 0:

        print(
            f"[{table_name}] "
            f"No hay datos nuevos "
            f"para Silver."
        )

        return

    # ========================================================
    # TRANSFORMAR SOLO LOS DATOS NUEVOS
    # ========================================================

    df_clean = transform_function(
        df_incremental
    )

    df_clean = (
        remove_exact_duplicates(
            df_clean
        )
    )

    cleaned_count = (
        df_clean.count()
    )

    print(
        f"[{table_name}] "
        f"Después de limpieza: "
        f"{cleaned_count}"
    )

    if cleaned_count == 0:

        print(
            f"[{table_name}] "
            f"No quedaron registros válidos."
        )

        return

    # ========================================================
    # EVITAR DUPLICADOS EXACTOS YA EXISTENTES EN SILVER
    # ========================================================

    df_to_insert = (
        remove_existing_exact_rows(
            df_new=df_clean,
            df_silver=df_silver,
        )
    )

    insert_count = (
        df_to_insert.count()
    )

    print(
        f"[{table_name}] "
        f"Versiones realmente nuevas: "
        f"{insert_count}"
    )

    if insert_count == 0:

        print(
            f"[{table_name}] "
            f"No hay versiones nuevas "
            f"para insertar."
        )

        return

    append_silver(
        df=df_to_insert,
        silver_path=silver_path,
    )

    print(
        f"[{table_name}] "
        f"{insert_count} registros "
        f"agregados a Silver."
    )