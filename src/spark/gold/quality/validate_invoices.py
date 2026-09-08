import os
import sys

from pyspark.sql import functions as F


CURRENT_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

GOLD_DIR = os.path.dirname(
    CURRENT_DIR
)

if GOLD_DIR not in sys.path:
    sys.path.append(GOLD_DIR)


from common import create_spark_session


SILVER_FACTURAS_PATH = "/opt/spark-data/silver/facturas"
SILVER_DETALLE_PATH = "/opt/spark-data/silver/detalles_factura"

QUARANTINE_PATH = "/opt/spark-data/quarantine/facturas"

# Diferencia máxima aceptada en valores monetarios.
TOLERANCIA_MONETARIA = 0.01


def validate_invoices():

    spark = create_spark_session(
        "gold_validate_invoices"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n==========================================")
        print("DATA QUALITY - RECONCILIACION DE FACTURAS")
        print("==========================================")

        # ====================================================
        # 1. LEER SILVER
        # ====================================================

        print("\nLeyendo Silver facturas...")

        df_facturas = (
            spark.read
            .format("delta")
            .load(SILVER_FACTURAS_PATH)
        )

        print("\nLeyendo Silver detalle_factura...")

        df_detalle = (
            spark.read
            .format("delta")
            .load(SILVER_DETALLE_PATH)
        )

        total_facturas = df_facturas.count()
        total_detalles = df_detalle.count()

        print(
            f"Facturas encontradas: {total_facturas}"
        )

        print(
            f"Líneas de detalle encontradas: {total_detalles}"
        )

        # ====================================================
        # 2. CALCULAR MEDIDAS A NIVEL DE LINEA
        #
        # Respetamos el grain futuro de fact_ventas:
        #
        # una fila = un producto dentro de una factura.
        # ====================================================

        df_detalle_calculado = (
            df_detalle

            .withColumn(
                "subtotal_linea_calculado",
                F.col("cantidad")
                * F.col("precio_unitario")
            )

            .withColumn(
                "descuento_linea_calculado",
                F.col("cantidad")
                * F.col("descuento_unitario")
            )

            .withColumn(
                "impuesto_linea_calculado",
                F.col("cantidad")
                * F.col("impuesto_unitario")
            )
        )

        # ====================================================
        # 3. AGRUPAR DETALLE POR FACTURA
        #
        # Reconstruimos los totales de cada factura desde
        # sus líneas.
        # ====================================================

        df_totales_detalle = (
            df_detalle_calculado

            .groupBy("factura_id")

            .agg(
                F.sum(
                    "subtotal_linea_calculado"
                ).alias(
                    "subtotal_calculado"
                ),

                F.sum(
                    "descuento_linea_calculado"
                ).alias(
                    "descuento_calculado"
                ),

                F.sum(
                    "impuesto_linea_calculado"
                ).alias(
                    "impuesto_calculado"
                ),

                F.sum(
                    "total_linea"
                ).alias(
                    "total_calculado"
                ),

                F.count(
                    "*"
                ).alias(
                    "cantidad_lineas"
                )
            )
        )

        # ====================================================
        # 4. UNIR CABECERA CON TOTALES CALCULADOS
        #
        # Usamos LEFT JOIN deliberadamente.
        #
        # Si existe una factura sin detalle también queremos
        # detectarla como inválida.
        # ====================================================

        df_validacion = (
            df_facturas.alias("f")

            .join(
                df_totales_detalle.alias("d"),
                on="factura_id",
                how="left",
            )
        )

        # ====================================================
        # 5. DETECTAR FACTURAS SIN DETALLE
        # ====================================================

        df_validacion = (
            df_validacion

            .withColumn(
                "sin_detalle",
                F.col(
                    "cantidad_lineas"
                ).isNull()
            )
        )

        # ====================================================
        # 6. COMPARAR SUBTOTAL
        # ====================================================

        df_validacion = (
            df_validacion

            .withColumn(
                "subtotal_valido",

                F.when(
                    F.col("subtotal_calculado").isNull(),
                    F.lit(False)
                )

                .otherwise(
                    F.abs(
                        F.col("subtotal")
                        -
                        F.col("subtotal_calculado")
                    )
                    <= F.lit(
                        TOLERANCIA_MONETARIA
                    )
                )
            )
        )

        # ====================================================
        # 7. COMPARAR DESCUENTO
        # ====================================================

        df_validacion = (
            df_validacion

            .withColumn(
                "descuento_valido",

                F.when(
                    F.col("descuento_calculado").isNull(),
                    F.lit(False)
                )

                .otherwise(
                    F.abs(
                        F.col("descuento_total")
                        -
                        F.col("descuento_calculado")
                    )
                    <= F.lit(
                        TOLERANCIA_MONETARIA
                    )
                )
            )
        )

        # ====================================================
        # 8. COMPARAR IMPUESTO
        # ====================================================

        df_validacion = (
            df_validacion

            .withColumn(
                "impuesto_valido",

                F.when(
                    F.col("impuesto_calculado").isNull(),
                    F.lit(False)
                )

                .otherwise(
                    F.abs(
                        F.col("impuesto_total")
                        -
                        F.col("impuesto_calculado")
                    )
                    <= F.lit(
                        TOLERANCIA_MONETARIA
                    )
                )
            )
        )

        # ====================================================
        # 9. COMPARAR TOTAL FINAL
        # ====================================================

        df_validacion = (
            df_validacion

            .withColumn(
                "total_valido",

                F.when(
                    F.col("total_calculado").isNull(),
                    F.lit(False)
                )

                .otherwise(
                    F.abs(
                        F.col("total")
                        -
                        F.col("total_calculado")
                    )
                    <= F.lit(
                        TOLERANCIA_MONETARIA
                    )
                )
            )
        )

        # ====================================================
        # 10. DETERMINAR SI LA FACTURA ES VALIDA
        # ====================================================

        df_validacion = (
            df_validacion

            .withColumn(
                "factura_valida",

                (
                    ~F.col("sin_detalle")
                    &
                    F.col("subtotal_valido")
                    &
                    F.col("descuento_valido")
                    &
                    F.col("impuesto_valido")
                    &
                    F.col("total_valido")
                )
            )
        )

        # ====================================================
        # 11. GENERAR MOTIVOS DE RECHAZO
        #
        # Una factura puede fallar por más de una razón.
        #
        # Ejemplo:
        #
        # SUBTOTAL_NO_COINCIDE,
        # TOTAL_NO_COINCIDE
        # ====================================================

        df_validacion = (
            df_validacion

            .withColumn(
                "motivos_rechazo",

                F.array_compact(
                    F.array(

                        F.when(
                            F.col("sin_detalle"),
                            F.lit(
                                "FACTURA_SIN_DETALLE"
                            )
                        ),

                        F.when(
                            ~F.col("subtotal_valido"),
                            F.lit(
                                "SUBTOTAL_NO_COINCIDE"
                            )
                        ),

                        F.when(
                            ~F.col("descuento_valido"),
                            F.lit(
                                "DESCUENTO_NO_COINCIDE"
                            )
                        ),

                        F.when(
                            ~F.col("impuesto_valido"),
                            F.lit(
                                "IMPUESTO_NO_COINCIDE"
                            )
                        ),

                        F.when(
                            ~F.col("total_valido"),
                            F.lit(
                                "TOTAL_NO_COINCIDE"
                            )
                        ),
                    )
                )
            )
        )

        # ====================================================
        # 12. FECHA DE VALIDACION
        # ====================================================

        df_validacion = (
            df_validacion
            .withColumn(
                "fecha_validacion",
                F.current_timestamp()
            )
        )

        # ====================================================
        # 13. SEPARAR VALIDAS E INVALIDAS
        # ====================================================

        df_validas = (
            df_validacion
            .filter(
                F.col("factura_valida")
            )
        )

        df_invalidas = (
            df_validacion
            .filter(
                ~F.col("factura_valida")
            )
        )

        # ====================================================
        # 14. PREPARAR QUARANTINE
        #
        # Guardamos información suficiente para investigar
        # posteriormente el problema.
        # ====================================================

        df_quarantine = (
            df_invalidas

            .select(
                "factura_id",
                "numero_factura",
                "cliente_id",
                "sucursal_id",
                "vendedor_id",
                "fecha_hora",

                "subtotal",
                "subtotal_calculado",

                "descuento_total",
                "descuento_calculado",

                "impuesto_total",
                "impuesto_calculado",

                "total",
                "total_calculado",

                "cantidad_lineas",

                "subtotal_valido",
                "descuento_valido",
                "impuesto_valido",
                "total_valido",

                "motivos_rechazo",
                "fecha_validacion",
            )
        )

        # ====================================================
        # 15. GUARDAR QUARANTINE
        #
        # Para esta primera implementación usamos overwrite.
        #
        # Cuando hagamos el pipeline incremental completo,
        # modificaremos esta estrategia para mantener el
        # historial de rechazos/reprocesamientos.
        # ====================================================

        print(
            "\nGuardando facturas inválidas "
            "en quarantine..."
        )

        (
            df_quarantine.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true"
            )
            .save(
                QUARANTINE_PATH
            )
        )

        # ====================================================
        # 16. ESTADISTICAS
        # ====================================================

        total_validas = df_validas.count()
        total_invalidas = df_invalidas.count()

        print("\n====================================")
        print("RESULTADO DE CALIDAD")
        print("====================================")

        print(
            f"Facturas procesadas : {total_facturas}"
        )

        print(
            f"Facturas válidas    : {total_validas}"
        )

        print(
            f"Facturas inválidas  : {total_invalidas}"
        )

        # ====================================================
        # 17. VALIDACION DE CONTROL
        # ====================================================

        if (
            total_validas
            +
            total_invalidas
            !=
            total_facturas
        ):

            raise ValueError(
                "La cantidad de facturas válidas + inválidas "
                "no coincide con las facturas procesadas."
            )

        # ====================================================
        # 18. MOSTRAR RECHAZOS
        # ====================================================

        if total_invalidas > 0:

            print(
                "\nEjemplos de facturas rechazadas:"
            )

            (
                df_quarantine
                .orderBy("factura_id")
                .show(
                    20,
                    truncate=False,
                )
            )

        else:

            print(
                "\nNo se encontraron facturas "
                "inconsistentes."
            )

        print(
            "\nValidación de facturas "
            "finalizada correctamente."
        )

        # ====================================================
        # 19. DEVOLVER FACTURAS VALIDAS
        #
        # Esto será útil cuando integremos esta lógica con
        # build_fact_ventas.py.
        # ====================================================

        return df_validas

    finally:

        spark.stop()


if __name__ == "__main__":
    validate_invoices()