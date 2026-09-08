from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import create_spark_session


SILVER_PATH = "/opt/spark-data/silver/sucursales"
GOLD_PATH = "/opt/spark-data/gold/dim_sucursal"


def build_dim_sucursal_initial():

    spark = create_spark_session(
        "gold_dim_sucursal_initial"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD - DIM_SUCURSAL")
        print("Carga inicial SCD")
        print("====================================")

        # ====================================================
        # 1. LEER SILVER
        # ====================================================

        print("\nLeyendo Silver sucursales...")

        df_silver = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
        )

        silver_count = df_silver.count()

        print(
            f"Registros encontrados: "
            f"{silver_count}"
        )

        # ====================================================
        # 2. VALIDAR detected_at
        #
        # La tabla sucursales no tiene updated_at en la fuente.
        #
        # Silver genera detected_at cuando:
        #
        # - encuentra una sucursal nueva
        # - detecta una modificación
        #
        # Por eso detected_at será nuestro orden temporal
        # para reconstruir las versiones observadas.
        # ====================================================

        if "detected_at" not in df_silver.columns:

            raise ValueError(
                "Silver sucursales no contiene "
                "la columna detected_at."
            )

        detected_at_nulls = (
            df_silver
            .filter(
                F.col("detected_at").isNull()
            )
            .count()
        )

        if detected_at_nulls > 0:

            raise ValueError(
                f"Se encontraron "
                f"{detected_at_nulls} registros "
                f"con detected_at NULL."
            )

        # ====================================================
        # 3. DEFINIR ORDEN DE TODAS LAS VERSIONES DE SILVER
        #
        # detected_at indica cuándo nuestro pipeline
        # detectó cada estado de la sucursal.
        # ====================================================

        version_window = (
            Window
            .partitionBy("sucursal_id")
            .orderBy(
                "detected_at"
            )
        )

        # ====================================================
        # 4. COMPARAR ATRIBUTOS SCD2
        #
        # SCD2:
        #
        # ciudad
        # direccion
        #
        # Silver puede tener versiones generadas por cambios
        # SCD1 también, por eso aquí todavía debemos decidir
        # cuáles realmente crean una versión dimensional.
        # ====================================================

        df_versions = (
            df_silver

            .withColumn(
                "_numero_fila_source",
                F.row_number().over(
                    version_window
                )
            )

            .withColumn(
                "ciudad_anterior",
                F.lag(
                    "ciudad"
                ).over(
                    version_window
                )
            )

            .withColumn(
                "direccion_anterior",
                F.lag(
                    "direccion"
                ).over(
                    version_window
                )
            )
        )

        # ====================================================
        # 5. DETECTAR CAMBIOS SCD2
        #
        # Primera fila observada:
        # siempre genera la primera versión dimensional.
        #
        # Posteriores:
        # solo crean nueva versión si cambia ciudad
        # o direccion.
        #
        # eqNullSafe permite comparar correctamente NULL.
        # ====================================================

        df_versions = (
            df_versions
            .withColumn(
                "cambio_scd2",
                (
                    (
                        F.col(
                            "_numero_fila_source"
                        ) == 1
                    )

                    |

                    (
                        ~F.col(
                            "ciudad"
                        ).eqNullSafe(
                            F.col(
                                "ciudad_anterior"
                            )
                        )
                    )

                    |

                    (
                        ~F.col(
                            "direccion"
                        ).eqNullSafe(
                            F.col(
                                "direccion_anterior"
                            )
                        )
                    )
                )
            )
        )

        # ====================================================
        # 6. CONSERVAR ÚNICAMENTE VERSIONES SCD2
        # ====================================================

        df_scd2 = (
            df_versions
            .filter(
                F.col("cambio_scd2")
            )
        )

        # ====================================================
        # 7. NUMERAR LAS VERSIONES SCD2 REALES
        #
        # IMPORTANTE:
        #
        # Numeramos DESPUÉS de eliminar cambios que fueron
        # solamente SCD1.
        #
        # Así sabemos cuál es realmente la primera versión
        # dimensional.
        # ====================================================

        scd_number_window = (
            Window
            .partitionBy("sucursal_id")
            .orderBy(
                "detected_at"
            )
        )

        df_scd2 = (
            df_scd2
            .withColumn(
                "_numero_version_scd2",
                F.row_number().over(
                    scd_number_window
                )
            )
        )

        # ====================================================
        # 8. VALIDAR fecha_apertura
        #
        # Para la primera versión conocida asumimos:
        #
        # la ciudad/dirección más antigua conocida estuvo
        # vigente desde fecha_apertura.
        #
        # Es una suposición explícita del modelo porque la
        # fuente no conserva historial previo a nuestra
        # plataforma.
        # ====================================================

        invalid_first_versions = (
            df_scd2
            .filter(
                (
                    F.col(
                        "_numero_version_scd2"
                    ) == 1
                )
                &
                (
                    F.col(
                        "fecha_apertura"
                    ).isNull()
                )
            )
            .count()
        )

        if invalid_first_versions > 0:

            raise ValueError(
                f"Se encontraron "
                f"{invalid_first_versions} sucursales "
                f"sin fecha_apertura en su "
                f"primera versión SCD2."
            )

        # ====================================================
        # 9. CALCULAR fecha_inicio
        #
        # Primera versión SCD2:
        #
        # fecha_inicio = fecha_apertura
        #
        # Versiones posteriores:
        #
        # fecha_inicio = detected_at
        #
        # detected_at representa cuándo nuestro pipeline
        # detectó el cambio, no necesariamente cuándo ocurrió
        # exactamente en PostgreSQL.
        # ====================================================

        df_scd2 = (
            df_scd2
            .withColumn(
                "fecha_inicio",
                F.when(
                    F.col(
                        "_numero_version_scd2"
                    ) == 1,
                    F.col(
                        "fecha_apertura"
                    ).cast(
                        "timestamp"
                    )
                )
                .otherwise(
                    F.col(
                        "detected_at"
                    )
                )
            )
        )

        # ====================================================
        # 10. CALCULAR fecha_fin Y es_actual
        #
        # fecha_fin de una versión =
        # fecha_inicio de la siguiente versión SCD2.
        # ====================================================

        validity_window = (
            Window
            .partitionBy("sucursal_id")
            .orderBy(
                "fecha_inicio"
            )
        )

        df_scd2 = (
            df_scd2

            .withColumn(
                "fecha_fin",
                F.lead(
                    "fecha_inicio"
                ).over(
                    validity_window
                )
            )

            .withColumn(
                "es_actual",
                F.col(
                    "fecha_fin"
                ).isNull()
            )
        )

        # ====================================================
        # 11. OBTENER LOS VALORES SCD1 MÁS RECIENTES
        #
        # SCD1:
        #
        # nombre
        # fecha_apertura
        # activa
        #
        # Estos atributos deben reflejar siempre el último
        # valor conocido en TODAS las versiones SCD2.
        # ====================================================

        latest_window = (
            Window
            .partitionBy("sucursal_id")
            .orderBy(
                F.col(
                    "detected_at"
                ).desc()
            )
        )

        df_latest = (
            df_silver

            .withColumn(
                "_rn",
                F.row_number().over(
                    latest_window
                )
            )

            .filter(
                F.col("_rn") == 1
            )

            .select(
                "sucursal_id",

                F.col(
                    "nombre"
                ).alias(
                    "latest_nombre"
                ),

                F.col(
                    "fecha_apertura"
                ).alias(
                    "latest_fecha_apertura"
                ),

                F.col(
                    "activa"
                ).alias(
                    "latest_activa"
                ),
            )
        )

        # ====================================================
        # 12. UNIR HISTORIAL SCD2 CON LOS VALORES SCD1
        #
        # De esta forma:
        #
        # ciudad/direccion conservan historia.
        #
        # nombre/fecha_apertura/activa toman siempre
        # el valor más reciente.
        # ====================================================

        df_gold = (
            df_scd2

            .join(
                df_latest,
                on="sucursal_id",
                how="left",
            )

            .withColumn(
                "nombre",
                F.col(
                    "latest_nombre"
                )
            )

            .withColumn(
                "fecha_apertura",
                F.col(
                    "latest_fecha_apertura"
                )
            )

            .withColumn(
                "activa",
                F.col(
                    "latest_activa"
                )
            )
        )

        # ====================================================
        # 13. GENERAR SURROGATE KEY
        #
        # Carga inicial:
        #
        # 1, 2, 3...
        #
        # Más adelante para incremental:
        #
        # MAX(sucursal_sk) + ROW_NUMBER()
        # ====================================================

        surrogate_window = (
            Window.orderBy(
                "sucursal_id",
                "fecha_inicio",
            )
        )

        df_gold = (
            df_gold
            .withColumn(
                "sucursal_sk",
                F.row_number().over(
                    surrogate_window
                ).cast(
                    "long"
                )
            )
        )

        # ====================================================
        # 14. TRAZABILIDAD
        #
        # La fuente sucursales NO posee updated_at.
        #
        # Para mantener el esquema técnico homogéneo con
        # nuestras otras dimensiones, source_updated_at
        # contiene aquí detected_at.
        #
        # Es decir:
        #
        # source_updated_at =
        # momento en que el pipeline detectó esa versión.
        #
        # No afirmamos que sea la hora exacta del cambio
        # en PostgreSQL.
        # ====================================================

        df_gold = (
            df_gold
            .withColumn(
                "source_updated_at",
                F.col(
                    "detected_at"
                )
            )
        )

        # ====================================================
        # 15. SELECCIONAR ESQUEMA FINAL GOLD
        # ====================================================

        df_gold = (
            df_gold
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

        # ====================================================
        # 16. VALIDACIONES
        # ====================================================

        # ----------------------------------------------------
        # Cada sucursal debe tener exactamente
        # una versión actual.
        # ----------------------------------------------------

        invalid_current = (
            df_gold
            .groupBy(
                "sucursal_id"
            )
            .agg(
                F.sum(
                    F.when(
                        F.col(
                            "es_actual"
                        ),
                        1
                    ).otherwise(
                        0
                    )
                ).alias(
                    "cantidad_actuales"
                )
            )
            .filter(
                F.col(
                    "cantidad_actuales"
                ) != 1
            )
        )

        invalid_current_count = (
            invalid_current.count()
        )

        if invalid_current_count > 0:

            print(
                "\nSucursales con cantidad "
                "incorrecta de versiones actuales:"
            )

            invalid_current.show(
                50,
                truncate=False,
            )

            raise ValueError(
                "Existen sucursales sin exactamente "
                "una versión actual."
            )

        # ----------------------------------------------------
        # fecha_fin siempre debe ser posterior
        # a fecha_inicio.
        # ----------------------------------------------------

        invalid_dates = (
            df_gold
            .filter(
                F.col(
                    "fecha_fin"
                ).isNotNull()
                &
                (
                    F.col(
                        "fecha_fin"
                    )
                    <=
                    F.col(
                        "fecha_inicio"
                    )
                )
            )
            .count()
        )

        if invalid_dates > 0:

            raise ValueError(
                f"Se encontraron "
                f"{invalid_dates} versiones "
                f"con fechas de vigencia inválidas."
            )

        # ====================================================
        # 17. GUARDAR DIMENSION EN DELTA
        # ====================================================

        print(
            "\nGuardando dim_sucursal..."
        )

        (
            df_gold.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true",
            )
            .save(
                GOLD_PATH
            )
        )

        gold_count = (
            df_gold.count()
        )

        print(
            f"Registros Gold creados: "
            f"{gold_count}"
        )

        # ====================================================
        # 18. MOSTRAR RESULTADO
        # ====================================================

        print(
            "\nEjemplo de dim_sucursal:"
        )

        (
            df_gold
            .orderBy(
                "sucursal_id",
                "fecha_inicio",
            )
            .show(
                50,
                truncate=False,
            )
        )

        print(
            "\ndim_sucursal creada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":

    build_dim_sucursal_initial()