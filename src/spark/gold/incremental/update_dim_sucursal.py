from delta.tables import DeltaTable

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import create_spark_session


SILVER_PATH = "/opt/spark-data/silver/sucursales"
GOLD_PATH = "/opt/spark-data/gold/dim_sucursal"


SCD1_COLUMNS = [
    "nombre",
    "fecha_apertura",
    "activa",
]

SCD2_COLUMNS = [
    "ciudad",
    "direccion",
]


def update_dim_sucursal():

    spark = create_spark_session(
        "gold_dim_sucursal_incremental"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD INCREMENTAL - DIM_SUCURSAL")
        print("====================================")

        # ====================================================
        # 1. LEER SILVER
        # ====================================================

        print("\nLeyendo sucursales Silver...")

        silver = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
        )

        # ====================================================
        # 2. LEER GOLD
        # ====================================================

        print("Leyendo dim_sucursal Gold...")

        gold = (
            spark.read
            .format("delta")
            .load(GOLD_PATH)
        )

        # ====================================================
        # 3. OBTENER ÚLTIMA VERSIÓN DE CADA SUCURSAL EN SILVER
        #
        # Silver trabaja con snapshot_compare.
        #
        # detected_at representa cuándo Silver detectó
        # el estado/cambio de la sucursal.
        # ====================================================

        silver_window = (
            Window
            .partitionBy(
                "sucursal_id"
            )
            .orderBy(
                F.col(
                    "detected_at"
                ).desc()
            )
        )

        silver_latest = (
            silver

            .withColumn(
                "rn",
                F.row_number().over(
                    silver_window
                )
            )

            .filter(
                F.col("rn") == 1
            )

            .drop("rn")
        )

        # ====================================================
        # 4. OBTENER VERSIÓN ACTUAL DE GOLD
        # ====================================================

        gold_actual = (
            gold

            .filter(
                F.col("es_actual") == True
            )
        )

        # ====================================================
        # 5. COMPARAR SILVER VS GOLD
        # ====================================================

        comparacion = (
            silver_latest.alias("s")

            .join(
                gold_actual.alias("g"),

                F.col(
                    "s.sucursal_id"
                )
                ==
                F.col(
                    "g.sucursal_id"
                ),

                "left",
            )

            .select(

                F.col("s.*"),

                F.col(
                    "g.sucursal_sk"
                ).alias(
                    "gold_sucursal_sk"
                ),

                F.col(
                    "g.nombre"
                ).alias(
                    "g_nombre"
                ),

                F.col(
                    "g.ciudad"
                ).alias(
                    "g_ciudad"
                ),

                F.col(
                    "g.direccion"
                ).alias(
                    "g_direccion"
                ),

                F.col(
                    "g.fecha_apertura"
                ).alias(
                    "g_fecha_apertura"
                ),

                F.col(
                    "g.activa"
                ).alias(
                    "g_activa"
                ),

                F.col(
                    "g.fecha_inicio"
                ).alias(
                    "g_fecha_inicio"
                ),

                # CORREGIDO:
                # En Gold la columna real se llama
                # source_updated_at.

                F.col(
                    "g.source_updated_at"
                ).alias(
                    "g_source_updated_at"
                ),
            )
        )

        # ====================================================
        # 6. SUCURSALES NUEVAS
        # ====================================================

        nuevas_sucursales = (
            comparacion

            .filter(
                F.col(
                    "gold_sucursal_sk"
                ).isNull()
            )
        )

        total_nuevas = (
            nuevas_sucursales.count()
        )

        print(
            f"\nSucursales nuevas: "
            f"{total_nuevas}"
        )

        # ====================================================
        # 7. CAMBIOS SCD2
        #
        # ciudad
        # direccion
        # ====================================================

        cambios_scd2 = (
            comparacion

            .filter(
                F.col(
                    "gold_sucursal_sk"
                ).isNotNull()
            )

            .filter(
                (
                    ~F.col(
                        "ciudad"
                    ).eqNullSafe(
                        F.col(
                            "g_ciudad"
                        )
                    )
                )
                |
                (
                    ~F.col(
                        "direccion"
                    ).eqNullSafe(
                        F.col(
                            "g_direccion"
                        )
                    )
                )
            )
        )

        total_scd2 = (
            cambios_scd2.count()
        )

        print(
            f"Cambios SCD2: "
            f"{total_scd2}"
        )

        # ====================================================
        # 8. CAMBIOS SCD1
        #
        # nombre
        # fecha_apertura
        # activa
        #
        # En esta implementación se procesan como SCD1-only
        # cuando no existe simultáneamente un cambio SCD2.
        # ====================================================

        cambios_scd1 = (
            comparacion

            .filter(
                F.col(
                    "gold_sucursal_sk"
                ).isNotNull()
            )

            # No debe existir cambio SCD2.

            .filter(
                F.col(
                    "ciudad"
                ).eqNullSafe(
                    F.col(
                        "g_ciudad"
                    )
                )
                &
                F.col(
                    "direccion"
                ).eqNullSafe(
                    F.col(
                        "g_direccion"
                    )
                )
            )

            # Debe existir al menos un cambio SCD1.

            .filter(
                (
                    ~F.col(
                        "nombre"
                    ).eqNullSafe(
                        F.col(
                            "g_nombre"
                        )
                    )
                )
                |
                (
                    ~F.col(
                        "fecha_apertura"
                    ).eqNullSafe(
                        F.col(
                            "g_fecha_apertura"
                        )
                    )
                )
                |
                (
                    ~F.col(
                        "activa"
                    ).eqNullSafe(
                        F.col(
                            "g_activa"
                        )
                    )
                )
            )
        )

        total_scd1 = (
            cambios_scd1.count()
        )

        print(
            f"Cambios solo SCD1: "
            f"{total_scd1}"
        )

        # ====================================================
        # 9. ABRIR DELTA TABLE
        # ====================================================

        delta_sucursal = (
            DeltaTable.forPath(
                spark,
                GOLD_PATH,
            )
        )

        # ====================================================
        # 10. APLICAR SCD1
        #
        # Los atributos SCD1 se sobrescriben en las versiones
        # históricas existentes.
        # ====================================================

        if total_scd1 > 0:

            print(
                "\nAplicando cambios SCD1..."
            )

            scd1_updates = (
                cambios_scd1

                .select(
                    "sucursal_id",
                    "nombre",
                    "fecha_apertura",
                    "activa",
                    "detected_at",
                )
            )

            (
                delta_sucursal.alias("g")

                .merge(
                    scd1_updates.alias("s"),

                    "g.sucursal_id = s.sucursal_id",
                )

                .whenMatchedUpdate(
                    set={
                        "nombre":
                            "s.nombre",

                        "fecha_apertura":
                            "s.fecha_apertura",

                        "activa":
                            "s.activa",

                        # CORREGIDO
                        "source_updated_at":
                            "s.detected_at",
                    }
                )

                .execute()
            )

        # ====================================================
        # 11. CERRAR VERSIONES ACTUALES SCD2
        # ====================================================

        if total_scd2 > 0:

            print(
                "\nCerrando versiones SCD2..."
            )

            scd2_close = (
                cambios_scd2

                .select(
                    "sucursal_id",
                    "detected_at",
                )
            )

            (
                delta_sucursal.alias("g")

                .merge(
                    scd2_close.alias("s"),

                    (
                        "g.sucursal_id = s.sucursal_id "
                        "AND g.es_actual = true"
                    ),
                )

                .whenMatchedUpdate(
                    set={
                        "fecha_fin":
                            "s.detected_at",

                        "es_actual":
                            "false",

                        # CORREGIDO
                        "source_updated_at":
                            "s.detected_at",
                    }
                )

                .execute()
            )

        # ====================================================
        # 12. OBTENER MAX SUCURSAL_SK
        # ====================================================

        max_sucursal_sk = (
            gold

            .agg(
                F.max(
                    "sucursal_sk"
                ).alias(
                    "max_sk"
                )
            )

            .collect()[0]["max_sk"]
        )

        if max_sucursal_sk is None:

            max_sucursal_sk = 0

        # ====================================================
        # 13. PREPARAR NUEVAS FILAS
        #
        # Pueden ser:
        #
        # - sucursal completamente nueva
        # - nueva versión SCD2
        # ====================================================

        filas_a_insertar = (
            nuevas_sucursales

            .withColumn(
                "tipo_insercion",
                F.lit(
                    "NUEVA_SUCURSAL"
                )
            )

            .unionByName(
                cambios_scd2

                .withColumn(
                    "tipo_insercion",
                    F.lit(
                        "NUEVA_VERSION_SCD2"
                    )
                ),

                allowMissingColumns=True,
            )
        )

        total_insertar = (
            filas_a_insertar.count()
        )

        # ====================================================
        # 14. INSERTAR NUEVAS VERSIONES
        # ====================================================

        if total_insertar > 0:

            print(
                f"\nFilas nuevas a insertar: "
                f"{total_insertar}"
            )

            insert_window = (
                Window.orderBy(
                    "sucursal_id",
                    "detected_at",
                )
            )

            nuevas_filas = (
                filas_a_insertar

                # ------------------------------------------------
                # Nueva surrogate key
                # ------------------------------------------------

                .withColumn(
                    "sucursal_sk",

                    (
                        F.lit(
                            max_sucursal_sk
                        )
                        +
                        F.row_number().over(
                            insert_window
                        )
                    )

                    .cast("long")
                )

                # ------------------------------------------------
                # FECHA INICIO
                #
                # Nueva sucursal:
                # fecha_apertura
                #
                # Nueva versión SCD2:
                # detected_at
                # ------------------------------------------------

                .withColumn(
                    "fecha_inicio",

                    F.when(
                        F.col(
                            "tipo_insercion"
                        )
                        ==
                        "NUEVA_SUCURSAL",

                        F.to_timestamp(
                            F.col(
                                "fecha_apertura"
                            )
                        )
                    )

                    .otherwise(
                        F.col(
                            "detected_at"
                        )
                    )
                )

                .withColumn(
                    "fecha_fin",

                    F.lit(
                        None
                    ).cast(
                        "timestamp"
                    )
                )

                .withColumn(
                    "es_actual",

                    F.lit(
                        True
                    )
                )

                # ------------------------------------------------
                # CORREGIDO
                #
                # Silver:
                # detected_at
                #
                # Gold:
                # source_updated_at
                # ------------------------------------------------

                .withColumn(
                    "source_updated_at",

                    F.col(
                        "detected_at"
                    )
                )

                # ------------------------------------------------
                # ESQUEMA FINAL EXACTO DE GOLD
                # ------------------------------------------------

                .select(
                    "sucursal_sk",
                    "sucursal_id",
                    "nombre",
                    "ciudad",
                    "direccion",
                    "fecha_apertura",
                    "activa",
                    "fecha_inicio",
                    "fecha_fin",
                    "es_actual",
                    "source_updated_at",
                )
            )

            (
                nuevas_filas.write
                .format("delta")
                .mode("append")
                .save(
                    GOLD_PATH
                )
            )

        # ====================================================
        # 15. VALIDACIONES POST-CARGA
        # ====================================================

        print(
            "\nValidando dim_sucursal..."
        )

        dim_final = (
            spark.read
            .format("delta")
            .load(GOLD_PATH)
        )

        # ----------------------------------------------------
        # VALIDAR SUCURSAL_SK ÚNICA
        # ----------------------------------------------------

        duplicate_sk = (
            dim_final

            .groupBy(
                "sucursal_sk"
            )

            .count()

            .filter(
                F.col(
                    "count"
                ) > 1
            )

            .count()
        )

        if duplicate_sk > 0:

            raise ValueError(
                "Se detectaron sucursal_sk duplicados."
            )

        # ----------------------------------------------------
        # EXACTAMENTE UNA VERSIÓN ACTUAL POR SUCURSAL
        # ----------------------------------------------------

        current_errors = (
            dim_final

            .filter(
                F.col(
                    "es_actual"
                ) == True
            )

            .groupBy(
                "sucursal_id"
            )

            .count()

            .filter(
                F.col(
                    "count"
                ) != 1
            )

            .count()
        )

        if current_errors > 0:

            raise ValueError(
                "Existen sucursales con una cantidad inválida "
                "de versiones actuales."
            )

        # ----------------------------------------------------
        # VALIDAR RANGOS TEMPORALES
        #
        # fecha_inicio nunca puede ser NULL.
        #
        # Si fecha_fin existe:
        #
        # fecha_inicio < fecha_fin
        # ----------------------------------------------------

        temporal_errors = (
            dim_final

            .filter(
                F.col(
                    "fecha_inicio"
                ).isNull()

                |

                (
                    F.col(
                        "fecha_fin"
                    ).isNotNull()

                    &

                    (
                        F.col(
                            "fecha_inicio"
                        )
                        >=
                        F.col(
                            "fecha_fin"
                        )
                    )
                )
            )

            .count()
        )

        if temporal_errors > 0:

            raise ValueError(
                "Se detectaron rangos temporales inválidos "
                "en dim_sucursal."
            )

        # ====================================================
        # 16. RESULTADO
        # ====================================================

        print("\n====================================")
        print("RESULTADO INCREMENTAL DIM_SUCURSAL")
        print("====================================")

        print(
            f"Sucursales nuevas: "
            f"{total_nuevas}"
        )

        print(
            f"Cambios SCD1: "
            f"{total_scd1}"
        )

        print(
            f"Cambios SCD2: "
            f"{total_scd2}"
        )

        print(
            f"Nuevas filas insertadas: "
            f"{total_insertar}"
        )

        print(
            f"Total filas dim_sucursal: "
            f"{dim_final.count()}"
        )

        print(
            "\nÚltimas filas de dim_sucursal:"
        )

        (
            dim_final

            .orderBy(
                F.col(
                    "sucursal_sk"
                ).desc()
            )

            .show(
                30,
                False,
            )
        )

        print(
            "\ndim_sucursal incremental "
            "procesada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    update_dim_sucursal()