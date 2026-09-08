from pyspark.sql import functions as F

from common import create_spark_session


GOLD_PATH = "/opt/spark-data/gold/dim_hora"


def build_dim_hora():

    spark = create_spark_session(
        "gold_dim_hora"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD - DIM_HORA")
        print("Generación dimensión de tiempo")
        print("====================================")

        # ====================================================
        # 1. GENERAR LOS 1440 MINUTOS DEL DÍA
        #
        # 24 horas * 60 minutos = 1440 registros
        #
        # Cada registro representa un minuto específico:
        #
        # 00:00
        # 00:01
        # 00:02
        # ...
        # 23:59
        # ====================================================

        df_hora = (
            spark
            .range(0, 24 * 60)
            .withColumnRenamed(
                "id",
                "minuto_del_dia"
            )
        )

        # ====================================================
        # 2. EXTRAER HORA
        #
        # División entera:
        #
        # minuto 0    -> hora 0
        # minuto 500  -> hora 8
        # minuto 1439 -> hora 23
        # ====================================================

        df_hora = (
            df_hora
            .withColumn(
                "hora",
                F.floor(
                    F.col("minuto_del_dia") / 60
                ).cast("int")
            )
        )

        # ====================================================
        # 3. EXTRAER MINUTO
        #
        # módulo 60
        #
        # 08:35 -> minuto 35
        # 14:20 -> minuto 20
        # ====================================================

        df_hora = (
            df_hora
            .withColumn(
                "minuto",
                (
                    F.col("minuto_del_dia") % 60
                ).cast("int")
            )
        )

        # ====================================================
        # 4. GENERAR SURROGATE KEY
        #
        # Utilizamos HHMM como clave determinística.
        #
        # 08:35 -> 835
        # 14:20 -> 1420
        #
        # Fórmula:
        #
        # hora * 100 + minuto
        # ====================================================

        df_hora = (
            df_hora
            .withColumn(
                "hora_sk",
                (
                    F.col("hora") * 100
                    +
                    F.col("minuto")
                ).cast("int")
            )
        )

        # ====================================================
        # 5. CREAR HORA COMPLETA
        #
        # La conservamos como texto HH:mm para facilitar
        # lectura y análisis.
        #
        # Ejemplo:
        #
        # 8 + 5 -> "08:05"
        # ====================================================

        df_hora = (
            df_hora
            .withColumn(
                "hora_completa",
                F.format_string(
                    "%02d:%02d",
                    F.col("hora"),
                    F.col("minuto"),
                )
            )
        )

        # ====================================================
        # 6. CREAR FRANJA HORARIA
        #
        # MADRUGADA : 00:00 - 05:59
        # MAÑANA    : 06:00 - 11:59
        # TARDE     : 12:00 - 17:59
        # NOCHE     : 18:00 - 23:59
        # ====================================================

        df_hora = (
            df_hora
            .withColumn(
                "franja_horaria",

                F.when(
                    F.col("hora").between(0, 5),
                    F.lit("MADRUGADA")
                )

                .when(
                    F.col("hora").between(6, 11),
                    F.lit("MAÑANA")
                )

                .when(
                    F.col("hora").between(12, 17),
                    F.lit("TARDE")
                )

                .otherwise(
                    F.lit("NOCHE")
                )
            )
        )

        # ====================================================
        # 7. ESQUEMA FINAL
        # ====================================================

        df_hora = (
            df_hora
            .select(
                "hora_sk",
                "hora",
                "minuto",
                "hora_completa",
                "franja_horaria",
            )
        )

        # ====================================================
        # 8. GUARDAR EN GOLD
        # ====================================================

        print("\nGuardando dim_hora...")

        (
            df_hora.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true"
            )
            .save(GOLD_PATH)
        )

        total_horas = df_hora.count()

        print(
            f"Registros creados: "
            f"{total_horas}"
        )

        # ====================================================
        # 9. VALIDACIÓN
        #
        # Esperamos exactamente 1440 registros.
        # ====================================================

        if total_horas != 1440:

            raise ValueError(
                "dim_hora debería contener "
                "exactamente 1440 registros."
            )

        # ====================================================
        # 10. MOSTRAR EJEMPLOS
        # ====================================================

        print(
            "\nPrimeros registros:"
        )

        (
            df_hora
            .orderBy(
                "hora",
                "minuto"
            )
            .show(
                20,
                truncate=False
            )
        )

        print(
            "\nEjemplos de diferentes "
            "franjas horarias:"
        )

        (
            df_hora
            .filter(
                F.col("hora_sk").isin(
                    0,
                    600,
                    1200,
                    1800,
                    2359,
                )
            )
            .orderBy("hora_sk")
            .show(
                truncate=False
            )
        )

        print(
            "\ndim_hora creada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    build_dim_hora()