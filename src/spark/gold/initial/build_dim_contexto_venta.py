from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import create_spark_session


SILVER_FACTURAS_PATH = "/opt/spark-data/silver/facturas"
GOLD_PATH = "/opt/spark-data/gold/dim_contexto_venta"


def build_dim_contexto_venta():

    spark = create_spark_session(
        "gold_dim_contexto_venta"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD - DIM_CONTEXTO_VENTA")
        print("Construcción Junk Dimension")
        print("====================================")

        # ====================================================
        # 1. LEER FACTURAS DESDE SILVER
        # ====================================================

        print("\nLeyendo Silver facturas...")

        df_facturas = (
            spark.read
            .format("delta")
            .load(SILVER_FACTURAS_PATH)
        )

        print(
            f"Facturas encontradas: "
            f"{df_facturas.count()}"
        )

        # ====================================================
        # 2. SELECCIONAR ATRIBUTOS DEL CONTEXTO DE VENTA
        #
        # Estos atributos vienen de la cabecera de factura.
        #
        # metodo_de_pago
        # estado
        #
        # Ambos describen el contexto de la transacción.
        # ====================================================

        df_contexto = (
            df_facturas
            .select(
                "metodo_de_pago",
                "estado",
            )
        )

        # ====================================================
        # 3. VALIDAR NULOS
        #
        # Para nuestra dimensión no queremos combinaciones
        # incompletas.
        #
        # Si en el futuro queremos manejar valores desconocidos
        # podemos introducir un registro UNKNOWN.
        # ====================================================

        df_contexto = (
            df_contexto
            .filter(
                F.col("metodo_de_pago").isNotNull()
                &
                F.col("estado").isNotNull()
            )
        )

        # ====================================================
        # 4. NORMALIZAR NUEVAMENTE POR SEGURIDAD
        #
        # Silver ya debería contener estos campos limpios,
        # pero Gold protege la consistencia de la dimensión.
        # ====================================================

        df_contexto = (
            df_contexto

            .withColumn(
                "metodo_de_pago",
                F.upper(
                    F.trim(
                        F.col("metodo_de_pago")
                    )
                )
            )

            .withColumn(
                "estado",
                F.upper(
                    F.trim(
                        F.col("estado")
                    )
                )
            )
        )

        # ====================================================
        # 5. OBTENER COMBINACIONES ÚNICAS
        #
        # Ejemplo:
        #
        # EFECTIVO        | PAGADA
        # TARJETA_DEBITO  | PAGADA
        # EFECTIVO        | CANCELADA
        #
        # Aunque existan miles de facturas con la misma
        # combinación, la dimensión guarda una sola fila.
        # ====================================================

        df_contexto = (
            df_contexto
            .dropDuplicates(
                [
                    "metodo_de_pago",
                    "estado",
                ]
            )
        )

        # ====================================================
        # 6. GENERAR SURROGATE KEY
        #
        # Ordenamos por los atributos para que la generación
        # inicial sea determinística.
        #
        # contexto_venta_sk:
        #
        # 1
        # 2
        # 3
        # ...
        # ====================================================

        surrogate_window = (
            Window.orderBy(
                "metodo_de_pago",
                "estado",
            )
        )

        df_contexto = (
            df_contexto
            .withColumn(
                "contexto_venta_sk",
                F.row_number().over(
                    surrogate_window
                ).cast("long")
            )
        )

        # ====================================================
        # 7. ESQUEMA FINAL
        # ====================================================

        df_contexto = (
            df_contexto.select(
                "contexto_venta_sk",
                "metodo_de_pago",
                "estado",
            )
        )

        # ====================================================
        # 8. GUARDAR GOLD
        # ====================================================

        print(
            "\nGuardando dim_contexto_venta..."
        )

        (
            df_contexto.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true"
            )
            .save(GOLD_PATH)
        )

        total_contextos = (
            df_contexto.count()
        )

        print(
            f"Contextos de venta creados: "
            f"{total_contextos}"
        )

        # ====================================================
        # 9. MOSTRAR DIMENSIÓN RESULTANTE
        # ====================================================

        print(
            "\nContenido de dim_contexto_venta:"
        )

        (
            df_contexto
            .orderBy(
                "contexto_venta_sk"
            )
            .show(
                100,
                truncate=False,
            )
        )

        # ====================================================
        # 10. VALIDAR UNICIDAD
        # ====================================================

        total_combinaciones = (
            df_contexto
            .select(
                "metodo_de_pago",
                "estado",
            )
            .distinct()
            .count()
        )

        if total_contextos != total_combinaciones:

            raise ValueError(
                "Se detectaron combinaciones duplicadas "
                "en dim_contexto_venta."
            )

        print(
            "\ndim_contexto_venta creada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    build_dim_contexto_venta()