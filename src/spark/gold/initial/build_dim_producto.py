from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import create_spark_session


SILVER_PATH = "/opt/spark-data/silver/productos"
GOLD_PATH = "/opt/spark-data/gold/dim_producto"

# ============================================================
# FECHA TÉCNICA PARA LA PRIMERA VERSIÓN CONOCIDA
#
# La fuente no contiene una fecha histórica real que indique
# desde cuándo existía el producto antes de ser migrado a la
# base de datos digital.
#
# Por eso usamos una fecha sentinel para que la primera versión
# conocida sea válida para todo el histórico anterior.
# ============================================================

FIRST_VERSION_DATE = "1900-01-01 00:00:00"


def build_dim_producto_initial():

    spark = create_spark_session(
        "gold_dim_producto_initial"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD - DIM_PRODUCTO")
        print("Carga inicial SCD")
        print("====================================")

        # ====================================================
        # 1. LEER SILVER
        # ====================================================

        print("\nLeyendo Silver productos...")

        df_silver = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
        )

        silver_count = (
            df_silver.count()
        )

        print(
            f"Registros encontrados: "
            f"{silver_count}"
        )

        # ====================================================
        # 2. VALIDAR updated_at
        #
        # updated_at es nuestra referencia temporal para
        # ordenar las versiones conocidas del producto.
        # ====================================================

        updated_at_nulls = (
            df_silver
            .filter(
                F.col("updated_at").isNull()
            )
            .count()
        )

        if updated_at_nulls > 0:

            raise ValueError(
                f"Se encontraron "
                f"{updated_at_nulls} productos "
                f"con updated_at NULL."
            )

        # ====================================================
        # 3. ORDENAR TODAS LAS VERSIONES POR PRODUCTO
        # ====================================================

        version_window = (
            Window
            .partitionBy("producto_id")
            .orderBy(
                "updated_at"
            )
        )

        # ====================================================
        # 4. OBTENER VALORES ANTERIORES DE ATRIBUTOS SCD2
        #
        # SCD2:
        #
        # categoria
        # subcategoria
        # marca
        # precio_venta
        # costo_unitario
        #
        # También numeramos TODAS las filas de Silver para
        # identificar correctamente la primera versión fuente.
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
                "categoria_anterior",
                F.lag(
                    "categoria"
                ).over(
                    version_window
                )
            )

            .withColumn(
                "subcategoria_anterior",
                F.lag(
                    "subcategoria"
                ).over(
                    version_window
                )
            )

            .withColumn(
                "marca_anterior",
                F.lag(
                    "marca"
                ).over(
                    version_window
                )
            )

            .withColumn(
                "precio_venta_anterior",
                F.lag(
                    "precio_venta"
                ).over(
                    version_window
                )
            )

            .withColumn(
                "costo_unitario_anterior",
                F.lag(
                    "costo_unitario"
                ).over(
                    version_window
                )
            )
        )

        # ====================================================
        # 5. DETECTAR CAMBIOS SCD2
        #
        # Primera fila del producto:
        # siempre crea la primera versión dimensional.
        #
        # Versiones posteriores:
        # solo crean nueva versión si cambia algún atributo
        # definido como SCD2.
        #
        # IMPORTANTE:
        # No usamos "categoria_anterior IS NULL" para detectar
        # la primera fila, porque categoria podría realmente
        # contener NULL.
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
                            "categoria"
                        ).eqNullSafe(
                            F.col(
                                "categoria_anterior"
                            )
                        )
                    )

                    |

                    (
                        ~F.col(
                            "subcategoria"
                        ).eqNullSafe(
                            F.col(
                                "subcategoria_anterior"
                            )
                        )
                    )

                    |

                    (
                        ~F.col(
                            "marca"
                        ).eqNullSafe(
                            F.col(
                                "marca_anterior"
                            )
                        )
                    )

                    |

                    (
                        ~F.col(
                            "precio_venta"
                        ).eqNullSafe(
                            F.col(
                                "precio_venta_anterior"
                            )
                        )
                    )

                    |

                    (
                        ~F.col(
                            "costo_unitario"
                        ).eqNullSafe(
                            F.col(
                                "costo_unitario_anterior"
                            )
                        )
                    )
                )
            )
        )

        # ====================================================
        # 6. CONSERVAR SOLO VERSIONES SCD2
        #
        # Si entre dos estados solo cambió un atributo SCD1,
        # esa fila no debe generar una nueva producto_sk.
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
        # Esto se hace DESPUÉS de filtrar.
        #
        # Así la versión SCD2 número 1 será realmente la
        # primera versión dimensional conocida.
        # ====================================================

        scd_number_window = (
            Window
            .partitionBy("producto_id")
            .orderBy(
                "updated_at"
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
        # 8. CALCULAR fecha_inicio
        #
        # PRIMERA VERSIÓN:
        #
        # No conocemos la fecha histórica real de inicio.
        #
        # Por eso:
        #
        # fecha_inicio = 1900-01-01
        #
        # VERSIONES POSTERIORES:
        #
        # fecha_inicio = updated_at
        #
        # Así, si existe una venta de 2024 o 2025,
        # podrá encontrar correctamente la primera producto_sk.
        # ====================================================

        df_scd2 = (
            df_scd2
            .withColumn(
                "fecha_inicio",
                F.when(
                    F.col(
                        "_numero_version_scd2"
                    ) == 1,
                    F.lit(
                        FIRST_VERSION_DATE
                    ).cast(
                        "timestamp"
                    )
                )
                .otherwise(
                    F.col(
                        "updated_at"
                    )
                )
            )
        )

        # ====================================================
        # 9. CALCULAR fecha_fin Y es_actual
        #
        # IMPORTANTE:
        #
        # fecha_fin debe ser la fecha_inicio de la siguiente
        # versión SCD2, no simplemente el siguiente updated_at
        # de cualquier fila de Silver.
        # ====================================================

        validity_window = (
            Window
            .partitionBy("producto_id")
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
        # 10. OBTENER ATRIBUTOS SCD1 MÁS RECIENTES
        #
        # SCD1:
        #
        # codigo_barras
        # nombre
        # activo
        #
        # Estos valores se actualizan en todas las versiones
        # históricas del producto.
        # ====================================================

        latest_window = (
            Window
            .partitionBy("producto_id")
            .orderBy(
                F.col(
                    "updated_at"
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
                "producto_id",

                F.col(
                    "codigo_barras"
                ).alias(
                    "latest_codigo_barras"
                ),

                F.col(
                    "nombre"
                ).alias(
                    "latest_nombre"
                ),

                F.col(
                    "activo"
                ).alias(
                    "latest_activo"
                ),
            )
        )

        # ====================================================
        # 11. COMBINAR HISTORIAL SCD2 + ATRIBUTOS SCD1
        # ====================================================

        df_gold = (
            df_scd2

            .join(
                df_latest,
                on="producto_id",
                how="left",
            )

            .withColumn(
                "codigo_barras",
                F.col(
                    "latest_codigo_barras"
                )
            )

            .withColumn(
                "nombre",
                F.col(
                    "latest_nombre"
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
        # 12. GENERAR SURROGATE KEY
        #
        # Carga inicial:
        #
        # producto_sk = 1, 2, 3...
        #
        # En incremental futuro:
        #
        # MAX(producto_sk) + ROW_NUMBER()
        # ====================================================

        surrogate_window = (
            Window.orderBy(
                "producto_id",
                "fecha_inicio",
            )
        )

        df_gold = (
            df_gold
            .withColumn(
                "producto_sk",
                F.row_number().over(
                    surrogate_window
                ).cast(
                    "long"
                )
            )
        )

        # ====================================================
        # 13. TRAZABILIDAD
        #
        # Conservamos el updated_at real de la fila fuente.
        #
        # IMPORTANTE:
        #
        # Para la primera versión:
        #
        # fecha_inicio      = 1900-01-01
        # source_updated_at = updated_at real
        #
        # No son contradictorios:
        #
        # fecha_inicio representa una vigencia técnica asumida.
        # source_updated_at conserva la trazabilidad real.
        # ====================================================

        df_gold = (
            df_gold
            .withColumn(
                "source_updated_at",
                F.col(
                    "updated_at"
                )
            )
        )

        # ====================================================
        # 14. ESQUEMA FINAL
        # ====================================================

        df_gold = (
            df_gold
            .select(
                "producto_sk",
                "producto_id",
                "codigo_barras",
                "nombre",

                "categoria",
                "subcategoria",
                "marca",
                "precio_venta",
                "costo_unitario",

                "activo",

                "fecha_inicio",
                "fecha_fin",
                "es_actual",
                "source_updated_at",
            )
        )

        # ====================================================
        # 15. VALIDACIONES
        # ====================================================

        # ----------------------------------------------------
        # Cada producto debe tener exactamente
        # una versión actual.
        # ----------------------------------------------------

        invalid_current = (
            df_gold
            .groupBy(
                "producto_id"
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
                "\nProductos con cantidad "
                "incorrecta de versiones actuales:"
            )

            invalid_current.show(
                50,
                truncate=False,
            )

            raise ValueError(
                "Existen productos sin exactamente "
                "una versión actual."
            )

        # ----------------------------------------------------
        # fecha_fin debe ser siempre posterior a fecha_inicio.
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
        # 16. GUARDAR GOLD
        # ====================================================

        print(
            "\nGuardando dim_producto..."
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
        # 17. MUESTRA
        # ====================================================

        print(
            "\nEjemplo de dim_producto:"
        )

        (
            df_gold
            .orderBy(
                "producto_id",
                "fecha_inicio",
            )
            .show(
                20,
                truncate=False,
            )
        )

        print(
            "\ndim_producto creada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":

    build_dim_producto_initial()