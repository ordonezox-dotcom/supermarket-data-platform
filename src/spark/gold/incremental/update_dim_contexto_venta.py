from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import create_spark_session


SILVER_FACTURAS_PATH = "/opt/spark-data/silver/facturas"
GOLD_PATH = "/opt/spark-data/gold/dim_contexto_venta"


def update_dim_contexto_venta():

    spark = create_spark_session(
        "gold_dim_contexto_venta_incremental"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n==========================================")
        print("GOLD INCREMENTAL - DIM_CONTEXTO_VENTA")
        print("==========================================")

        # ====================================================
        # 1. LEER FACTURAS SILVER
        # ====================================================

        print("\nLeyendo facturas Silver...")

        facturas = (
            spark.read
            .format("delta")
            .load(SILVER_FACTURAS_PATH)
        )

        # ====================================================
        # 2. LEER DIM_CONTEXTO_VENTA
        # ====================================================

        print("Leyendo dim_contexto_venta Gold...")

        dim_contexto = (
            spark.read
            .format("delta")
            .load(GOLD_PATH)
        )

        # ====================================================
        # 3. OBTENER COMBINACIONES PRESENTES EN SILVER
        #
        # El contexto de venta está definido por:
        #
        # metodo_de_pago + estado
        # ====================================================

        contextos_silver = (
            facturas

            .select(
                "metodo_de_pago",
                "estado",
            )

            .filter(
                F.col("metodo_de_pago").isNotNull()
                &
                F.col("estado").isNotNull()
            )

            .distinct()
        )

        print(
            "\nContextos distintos encontrados en Silver:"
        )

        contextos_silver.show(
            truncate=False
        )

        # ====================================================
        # 4. IDENTIFICAR CONTEXTOS NUEVOS
        #
        # left_anti devuelve únicamente combinaciones que
        # existen en Silver pero todavía no existen en Gold.
        # ====================================================

        nuevos_contextos = (
            contextos_silver.alias("s")

            .join(
                dim_contexto.alias("g"),

                (
                    F.col("s.metodo_de_pago")
                    ==
                    F.col("g.metodo_de_pago")
                )
                &
                (
                    F.col("s.estado")
                    ==
                    F.col("g.estado")
                ),

                "left_anti",
            )
        )

        total_nuevos = (
            nuevos_contextos.count()
        )

        print(
            f"\nContextos nuevos encontrados: "
            f"{total_nuevos}"
        )

        # ====================================================
        # 5. OBTENER SK MÁXIMA ACTUAL
        # ====================================================

        max_contexto_sk = (
            dim_contexto

            .agg(
                F.max(
                    "contexto_venta_sk"
                ).alias(
                    "max_sk"
                )
            )

            .collect()[0]["max_sk"]
        )

        if max_contexto_sk is None:
            max_contexto_sk = 0

        # ====================================================
        # 6. GENERAR NUEVAS SURROGATE KEYS
        # ====================================================

        if total_nuevos > 0:

            print(
                "\nGenerando nuevos contexto_venta_sk..."
            )

            window_sk = (
                Window.orderBy(
                    "metodo_de_pago",
                    "estado",
                )
            )

            nuevos_contextos = (
                nuevos_contextos

                .withColumn(
                    "contexto_venta_sk",

                    (
                        F.lit(max_contexto_sk)
                        +
                        F.row_number().over(
                            window_sk
                        )
                    ).cast("long")
                )

                .select(
                    "contexto_venta_sk",
                    "metodo_de_pago",
                    "estado",
                )
            )

            print(
                "\nNuevos contextos a insertar:"
            )

            nuevos_contextos.show(
                truncate=False
            )

            # ================================================
            # 7. INSERTAR EN GOLD
            # ================================================

            (
                nuevos_contextos.write
                .format("delta")
                .mode("append")
                .save(GOLD_PATH)
            )

        # ====================================================
        # 8. VOLVER A LEER GOLD
        # ====================================================

        dim_final = (
            spark.read
            .format("delta")
            .load(GOLD_PATH)
        )

        # ====================================================
        # 9. VALIDAR SK ÚNICA
        # ====================================================

        duplicate_sk = (
            dim_final

            .groupBy(
                "contexto_venta_sk"
            )

            .count()

            .filter(
                F.col("count") > 1
            )

            .count()
        )

        if duplicate_sk > 0:

            raise ValueError(
                "Se detectaron contexto_venta_sk duplicadas."
            )

        # ====================================================
        # 10. VALIDAR COMBINACIONES ÚNICAS
        #
        # No pueden existir dos SK diferentes para:
        #
        # EFECTIVO + PAGADA
        # ====================================================

        duplicate_context = (
            dim_final

            .groupBy(
                "metodo_de_pago",
                "estado",
            )

            .count()

            .filter(
                F.col("count") > 1
            )

            .count()
        )

        if duplicate_context > 0:

            raise ValueError(
                "Existen combinaciones duplicadas de "
                "metodo_de_pago + estado."
            )

        # ====================================================
        # 11. VALIDAR COBERTURA DE SILVER
        #
        # Después del incremental, toda combinación utilizada
        # por las facturas debe existir en la dimensión.
        # ====================================================

        contextos_sin_dimension = (
            contextos_silver.alias("s")

            .join(
                dim_final.alias("g"),

                (
                    F.col("s.metodo_de_pago")
                    ==
                    F.col("g.metodo_de_pago")
                )
                &
                (
                    F.col("s.estado")
                    ==
                    F.col("g.estado")
                ),

                "left_anti",
            )

            .count()
        )

        if contextos_sin_dimension > 0:

            raise ValueError(
                f"Existen {contextos_sin_dimension} contextos "
                "de venta de Silver sin correspondencia "
                "en dim_contexto_venta."
            )

        # ====================================================
        # 12. VALIDAR NULOS
        # ====================================================

        null_errors = (
            dim_final

            .filter(
                F.col(
                    "contexto_venta_sk"
                ).isNull()
                |
                F.col(
                    "metodo_de_pago"
                ).isNull()
                |
                F.col(
                    "estado"
                ).isNull()
            )

            .count()
        )

        if null_errors > 0:

            raise ValueError(
                "dim_contexto_venta contiene valores "
                "NULL no permitidos."
            )

        # ====================================================
        # 13. RESULTADO
        # ====================================================

        print("\n==========================================")
        print("RESULTADO INCREMENTAL DIM_CONTEXTO_VENTA")
        print("==========================================")

        print(
            f"Contextos nuevos insertados: "
            f"{total_nuevos}"
        )

        print(
            f"Total contextos en Gold: "
            f"{dim_final.count()}"
        )

        print(
            "\nContenido actual de dim_contexto_venta:"
        )

        (
            dim_final

            .orderBy(
                "contexto_venta_sk"
            )

            .show(
                truncate=False
            )
        )

        print(
            "\ndim_contexto_venta incremental "
            "procesada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    update_dim_contexto_venta()