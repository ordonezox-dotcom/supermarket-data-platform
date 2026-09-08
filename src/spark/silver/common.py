from delta.tables import DeltaTable

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# LECTURA DELTA
# ============================================================

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


# ============================================================
# FUNCIONES PARA CARGA INCREMENTAL
# ============================================================

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


# ============================================================
# LIMPIEZA GENERAL
# ============================================================

def remove_exact_duplicates(df):
    return df.dropDuplicates()


# ============================================================
# ESCRITURA SILVER
# ============================================================

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


# ============================================================
# EVITAR DUPLICADOS EXACTOS
# PARA TABLAS INCREMENTALES
# ============================================================

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


# ============================================================
# VALIDAR CLAVE ÚNICA EN SNAPSHOT
# ============================================================

def validate_snapshot_key(
    df,
    key_column: str,
    table_name: str,
):
    null_keys = (
        df
        .filter(
            F.col(key_column).isNull()
        )
        .count()
    )

    if null_keys > 0:

        raise ValueError(
            f"[{table_name}] "
            f"Se encontraron {null_keys} "
            f"registros con {key_column} NULL."
        )

    duplicated_keys = (
        df
        .groupBy(key_column)
        .count()
        .filter(
            F.col("count") > 1
        )
    )

    duplicated_count = (
        duplicated_keys.count()
    )

    if duplicated_count > 0:

        print(
            f"[{table_name}] "
            f"Claves duplicadas encontradas "
            f"en snapshot:"
        )

        duplicated_keys.show(
            50,
            truncate=False,
        )

        raise ValueError(
            f"[{table_name}] "
            f"El snapshot debe contener "
            f"una sola fila por {key_column}."
        )


# ============================================================
# OBTENER EL ÚLTIMO ESTADO CONOCIDO EN SILVER
# ============================================================

def get_latest_silver_state(
    df_silver,
    key_column: str,
):
    window_latest = (
        Window
        .partitionBy(key_column)
        .orderBy(
            F.col(
                "detected_at"
            ).desc()
        )
    )

    return (
        df_silver
        .withColumn(
            "_row_number",
            F.row_number().over(
                window_latest
            ),
        )
        .filter(
            F.col(
                "_row_number"
            ) == 1
        )
        .drop(
            "_row_number"
        )
    )


# ============================================================
# COMPARAR SNAPSHOT DE BRONZE
# CONTRA ÚLTIMO ESTADO DE SILVER
# ============================================================

def get_snapshot_changes(
    df_current,
    df_silver,
    key_column: str,
):
    """
    Compara el snapshot actual proveniente de Bronze
    contra el último estado conocido de cada registro
    almacenado en Silver.

    Retorna únicamente:

    - registros nuevos
    - registros existentes que cambiaron

    No decide si el cambio es SCD1 o SCD2.
    Esa responsabilidad pertenece a Gold.
    """

    current_columns = (
        df_current.columns
    )

    comparison_columns = [
        column
        for column
        in current_columns
        if column != key_column
    ]

    df_latest = (
        get_latest_silver_state(
            df_silver=df_silver,
            key_column=key_column,
        )
    )

    # --------------------------------------------------------
    # Renombramos las columnas de Silver para evitar
    # colisiones durante la comparación.
    # --------------------------------------------------------

    old_columns = [

        F.col(key_column)
        .alias(
            "_old_key"
        )

    ]

    for column in comparison_columns:

        old_columns.append(

            F.col(column)
            .alias(
                f"_old_{column}"
            )

        )

    df_latest_renamed = (
        df_latest
        .select(
            *old_columns
        )
    )

    # --------------------------------------------------------
    # LEFT JOIN:
    #
    # Conservamos todas las filas del snapshot actual
    # y buscamos su versión anterior en Silver.
    # --------------------------------------------------------

    df_comparison = (
        df_current.alias("new")
        .join(
            df_latest_renamed.alias(
                "old"
            ),
            F.col(
                f"new.{key_column}"
            )
            == F.col(
                "old._old_key"
            ),
            how="left",
        )
    )

    # --------------------------------------------------------
    # CASO 1:
    #
    # No existe _old_key
    # -> registro nuevo
    # --------------------------------------------------------

    change_condition = (
        F.col(
            "old._old_key"
        ).isNull()
    )

    # --------------------------------------------------------
    # CASO 2:
    #
    # El registro existe, pero una o varias columnas
    # tienen valores diferentes.
    #
    # eqNullSafe permite comparar correctamente NULL.
    # --------------------------------------------------------

    for column in comparison_columns:

        column_changed = (

            ~F.col(
                f"new.{column}"
            ).eqNullSafe(
                F.col(
                    f"old._old_{column}"
                )
            )

        )

        change_condition = (
            change_condition
            | column_changed
        )

    # --------------------------------------------------------
    # Conservamos solamente las columnas del snapshot nuevo.
    # --------------------------------------------------------

    return (
        df_comparison
        .filter(
            change_condition
        )
        .select(
            "new.*"
        )
    )


# ============================================================
# AGREGAR FECHA DE DETECCIÓN
# ============================================================

def add_detected_at(df):
    return (
        df
        .withColumn(
            "detected_at",
            F.current_timestamp(),
        )
    )


# ============================================================
# PROCESAR TABLA
# ============================================================

def process_table(
    spark,
    table_name: str,
    transform_function,
    strategy: str = "incremental",
    cursor_column: str = None,
    key_column: str = None,
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
        f"Estrategia: {strategy}"
    )

    if cursor_column is not None:

        print(
            f"Cursor: {cursor_column}"
        )

    if key_column is not None:

        print(
            f"Clave de comparación: "
            f"{key_column}"
        )

    print(
        "===================================="
    )

    df_bronze = (
        load_bronze_delta(
            spark,
            bronze_path,
        )
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

        df_clean = (
            transform_function(
                df_bronze
            )
        )

        df_clean = (
            remove_exact_duplicates(
                df_clean
            )
        )

        # ----------------------------------------------------
        # SNAPSHOT_COMPARE
        #
        # En primera carga todavía no hay Silver con el cual
        # comparar.
        #
        # Todo el snapshot actual es el primer estado conocido.
        # ----------------------------------------------------

        if strategy == "snapshot_compare":

            if key_column is None:

                raise ValueError(
                    f"[{table_name}] "
                    f"snapshot_compare requiere "
                    f"key_column."
                )

            validate_snapshot_key(
                df=df_clean,
                key_column=key_column,
                table_name=table_name,
            )

            df_clean = (
                add_detected_at(
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
    # SILVER YA EXISTE
    # ========================================================

    print(
        f"[{table_name}] "
        f"Silver existente."
    )

    df_silver = (
        load_silver_delta(
            spark,
            silver_path,
        )
    )

    # ========================================================
    # ESTRATEGIA 1:
    # INCREMENTAL POR CURSOR
    # ========================================================

    if strategy == "incremental":

        if cursor_column is None:

            raise ValueError(
                f"[{table_name}] "
                f"La estrategia incremental "
                f"requiere cursor_column."
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
                last_cursor_value=(
                    last_cursor_value
                ),
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

        # ----------------------------------------------------
        # TRANSFORMAR SOLAMENTE NUEVOS
        # ----------------------------------------------------

        df_clean = (
            transform_function(
                df_incremental
            )
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

        # ----------------------------------------------------
        # EVITAR DUPLICADOS EXACTOS
        # ----------------------------------------------------

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

        return

    # ========================================================
    # ESTRATEGIA 2:
    # SNAPSHOT + COMPARACIÓN
    # ========================================================

    if strategy == "snapshot_compare":

        if key_column is None:

            raise ValueError(
                f"[{table_name}] "
                f"snapshot_compare requiere "
                f"key_column."
            )

        print(
            f"[{table_name}] "
            f"Procesando snapshot completo "
            f"de Bronze."
        )

        # ----------------------------------------------------
        # Transformamos TODO Bronze porque Bronze representa
        # el estado actual completo de la tabla.
        # ----------------------------------------------------

        df_clean = (
            transform_function(
                df_bronze
            )
        )

        df_clean = (
            remove_exact_duplicates(
                df_clean
            )
        )

        validate_snapshot_key(
            df=df_clean,
            key_column=key_column,
            table_name=table_name,
        )

        snapshot_count = (
            df_clean.count()
        )

        print(
            f"[{table_name}] "
            f"Registros en snapshot actual: "
            f"{snapshot_count}"
        )

        # ----------------------------------------------------
        # Comparamos el snapshot actual de Bronze
        # con el último estado conocido de Silver.
        # ----------------------------------------------------

        df_changes = (
            get_snapshot_changes(
                df_current=df_clean,
                df_silver=df_silver,
                key_column=key_column,
            )
        )

        changes_count = (
            df_changes.count()
        )

        print(
            f"[{table_name}] "
            f"Registros nuevos o modificados: "
            f"{changes_count}"
        )

        if changes_count == 0:

            print(
                f"[{table_name}] "
                f"Snapshot sin cambios."
            )

            return

        # ----------------------------------------------------
        # Como PostgreSQL no tiene updated_at,
        # registramos cuándo nuestro pipeline detectó
        # esta nueva versión.
        # ----------------------------------------------------

        df_to_insert = (
            add_detected_at(
                df_changes
            )
        )

        append_silver(
            df=df_to_insert,
            silver_path=silver_path,
        )

        print(
            f"[{table_name}] "
            f"{changes_count} nuevas versiones "
            f"agregadas a Silver."
        )

        return

    # ========================================================
    # ESTRATEGIA DESCONOCIDA
    # ========================================================

    raise ValueError(
        f"[{table_name}] "
        f"Estrategia desconocida: "
        f"{strategy}"
    )