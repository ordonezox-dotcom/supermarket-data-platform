from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import create_spark_session


SILVER_PATH = "/opt/spark-data/silver/vendedores"
GOLD_PATH = "/opt/spark-data/gold/dim_vendedor"


def build_dim_vendedor_initial():

    spark = create_spark_session(
        "gold_dim_vendedor_initial"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD - DIM_VENDEDOR")
        print("Carga inicial SCD")
        print("====================================")

        # ====================================================
        # 1. LEER SILVER
        # ====================================================

        print("\nLeyendo Silver vendedores...")

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
        # La fuente vendedores no tiene updated_at.
        #
        # Silver genera detected_at cuando:
        #
        # - aparece un vendedor nuevo
        # - detecta un cambio sobre un vendedor existente
        #
        # detected_at será nuestra referencia temporal para
        # ordenar los estados observados.
        # ====================================================

        if "detected_at" not in df_silver.columns:

            raise ValueError(
                "Silver vendedores no contiene "
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
        # 3. DEFINIR ORDEN DE TODAS LAS VERSIONES
        #
        # Un vendedor puede aparecer varias veces en Silver
        # porque Silver conserva los cambios detectados.
        # ====================================================

        version_window = (
            Window
            .partitionBy("vendedor_id")
            .orderBy(
                "detected_at"
            )
        )

        # ====================================================
        # 4. OBTENER VALOR ANTERIOR DEL ATRIBUTO SCD2
        #
        # SCD2:
        #
        # sucursal_id
        #
        # Si el vendedor cambia de sucursal queremos conservar
        # ambas asignaciones como versiones dimensionales.
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
                "sucursal_id_anterior",
                F.lag(
                    "sucursal_id"
                ).over(
                    version_window
                )
            )
        )

        # ====================================================
        # 5. DETECTAR CAMBIOS SCD2
        #
        # Primera fila:
        # siempre genera primera versión dimensional.
        #
        # Versiones posteriores:
        # solamente crean otra versión si cambia sucursal_id.
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
                            "sucursal_id"
                        ).eqNullSafe(
                            F.col(
                                "sucursal_id_anterior"
                            )
                        )
                    )
                )
            )
        )

        # ====================================================
        # 6. CONSERVAR ÚNICAMENTE VERSIONES SCD2
        #
        # Silver también puede conservar cambios SCD1 como:
        #
        # correo
        # nombre
        # activo
        #
        # Esos cambios NO crean una nueva vendedor_sk.
        # ====================================================

        df_scd2 = (
            df_versions
            .filter(
                F.col("cambio_scd2")
            )
        )

        # ====================================================
        # 7. NUMERAR VERSIONES SCD2 REALES
        #
        # IMPORTANTE:
        #
        # La numeración ocurre DESPUÉS de eliminar filas
        # causadas únicamente por cambios SCD1.
        # ====================================================

        scd_number_window = (
            Window
            .partitionBy("vendedor_id")
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
        # 8. VALIDAR fecha_de_contratacion
        #
        # Para la primera versión conocida asumimos que
        # sucursal_id estuvo vigente desde la contratación.
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
                        "fecha_de_contratacion"
                    ).isNull()
                )
            )
            .count()
        )

        if invalid_first_versions > 0:

            raise ValueError(
                f"Se encontraron "
                f"{invalid_first_versions} vendedores "
                f"sin fecha_de_contratacion en su "
                f"primera versión SCD2."
            )

        # ====================================================
        # 9. CALCULAR fecha_inicio
        #
        # Primera versión:
        #
        # fecha_inicio = fecha_de_contratacion
        #
        # Versiones posteriores:
        #
        # fecha_inicio = detected_at
        #
        # detected_at representa cuándo nuestro pipeline
        # detectó el cambio de estado.
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
                        "fecha_de_contratacion"
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
        # fecha_fin =
        # fecha_inicio de la siguiente versión SCD2.
        # ====================================================

        validity_window = (
            Window
            .partitionBy("vendedor_id")
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
        # 11. OBTENER ATRIBUTOS SCD1 MÁS RECIENTES
        #
        # SCD1:
        #
        # documento
        # nombre
        # apellidos
        # correo
        # fecha_de_contratacion
        # activo
        #
        # El último valor conocido debe aparecer en todas las
        # versiones SCD2 del vendedor.
        # ====================================================

        latest_window = (
            Window
            .partitionBy("vendedor_id")
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
                "vendedor_id",

                F.col(
                    "documento"
                ).alias(
                    "latest_documento"
                ),

                F.col(
                    "nombre"
                ).alias(
                    "latest_nombre"
                ),

                F.col(
                    "apellidos"
                ).alias(
                    "latest_apellidos"
                ),

                F.col(
                    "correo"
                ).alias(
                    "latest_correo"
                ),

                F.col(
                    "fecha_de_contratacion"
                ).alias(
                    "latest_fecha_de_contratacion"
                ),

                F.col(
                    "activo"
                ).alias(
                    "latest_activo"
                ),
            )
        )

        # ====================================================
        # 12. COMBINAR HISTORIAL SCD2 + ATRIBUTOS SCD1
        # ====================================================

        df_gold = (
            df_scd2

            .join(
                df_latest,
                on="vendedor_id",
                how="left",
            )

            .withColumn(
                "documento",
                F.col(
                    "latest_documento"
                )
            )

            .withColumn(
                "nombre",
                F.col(
                    "latest_nombre"
                )
            )

            .withColumn(
                "apellidos",
                F.col(
                    "latest_apellidos"
                )
            )

            .withColumn(
                "correo",
                F.col(
                    "latest_correo"
                )
            )

            .withColumn(
                "fecha_de_contratacion",
                F.col(
                    "latest_fecha_de_contratacion"
                )
            )

            .withColumn(
                "activo",
                F.col(
                    "latest_activo"
                )
            )
        )

        # ====================================================
        # 13. GENERAR SURROGATE KEY
        #
        # Carga inicial:
        #
        # vendedor_sk = 1, 2, 3...
        #
        # Incremental futuro:
        #
        # MAX(vendedor_sk) + ROW_NUMBER()
        # ====================================================

        surrogate_window = (
            Window.orderBy(
                "vendedor_id",
                "fecha_inicio",
            )
        )

        df_gold = (
            df_gold
            .withColumn(
                "vendedor_sk",
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
        # La fuente vendedores no tiene updated_at.
        #
        # Conservamos source_updated_at para mantener un
        # esquema similar al resto de dimensiones.
        #
        # En este caso contiene detected_at.
        #
        # Por tanto representa el momento de detección por
        # nuestro pipeline y NO necesariamente el momento
        # exacto en que ocurrió el cambio en PostgreSQL.
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
        # 15. ESQUEMA FINAL
        # ====================================================

        df_gold = (
            df_gold
            .select(
                "vendedor_sk",
                "vendedor_id",
                "sucursal_id",
                "documento",
                "nombre",
                "apellidos",
                "correo",
                "fecha_de_contratacion",
                "activo",
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
        # Cada vendedor debe tener exactamente una
        # versión actual.
        # ----------------------------------------------------

        invalid_current = (
            df_gold
            .groupBy(
                "vendedor_id"
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
                "\nVendedores con cantidad "
                "incorrecta de versiones actuales:"
            )

            invalid_current.show(
                50,
                truncate=False,
            )

            raise ValueError(
                "Existen vendedores sin exactamente "
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
        # 17. GUARDAR GOLD
        # ====================================================

        print(
            "\nGuardando dim_vendedor..."
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
            "\nEjemplo de dim_vendedor:"
        )

        (
            df_gold
            .orderBy(
                "vendedor_id",
                "fecha_inicio",
            )
            .show(
                50,
                truncate=False,
            )
        )

        print(
            "\ndim_vendedor creada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":

    build_dim_vendedor_initial()