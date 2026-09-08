from pyspark.sql import functions as F

from common import create_spark_session


SILVER_FACTURAS_PATH = "/opt/spark-data/silver/facturas"
GOLD_PATH = "/opt/spark-data/gold/dim_fecha"


def update_dim_fecha():

    spark = create_spark_session(
        "gold_dim_fecha_incremental"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD INCREMENTAL - DIM_FECHA")
        print("====================================")

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
        # 2. LEER DIM_FECHA ACTUAL
        # ====================================================

        print("Leyendo dim_fecha Gold...")

        dim_fecha = (
            spark.read
            .format("delta")
            .load(GOLD_PATH)
        )

        # ====================================================
        # 3. EXTRAER FECHAS PRESENTES EN SILVER
        # ====================================================

        fechas_silver = (
            facturas

            .select(
                F.to_date(
                    "fecha_hora"
                ).alias("fecha")
            )

            .filter(
                F.col("fecha").isNotNull()
            )

            .distinct()
        )

        # ====================================================
        # 4. IDENTIFICAR FECHAS QUE NO EXISTEN EN GOLD
        #
        # left_anti:
        # devuelve únicamente las fechas de Silver que
        # todavía no tienen correspondencia en dim_fecha.
        # ====================================================

        nuevas_fechas = (
            fechas_silver.alias("s")

            .join(
                dim_fecha
                .select("fecha")
                .alias("g"),

                F.col("s.fecha")
                ==
                F.col("g.fecha"),

                "left_anti",
            )
        )

        total_nuevas = (
            nuevas_fechas.count()
        )

        print(
            f"\nFechas nuevas encontradas: "
            f"{total_nuevas}"
        )

        # ====================================================
        # 5. CONSTRUIR ATRIBUTOS DE DIM_FECHA
        # ====================================================

        if total_nuevas > 0:

            print(
                "\nConstruyendo nuevas fechas..."
            )

            nuevas_filas = (
                nuevas_fechas

                # YYYYMMDD
                .withColumn(
                    "fecha_sk",
                    F.date_format(
                        F.col("fecha"),
                        "yyyyMMdd"
                    ).cast("int")
                )

                .withColumn(
                    "anio",
                    F.year("fecha")
                )

                .withColumn(
                    "mes",
                    F.month("fecha")
                )

                .withColumn(
                    "dia",
                    F.dayofmonth("fecha")
                )

                .withColumn(
                    "trimestre",
                    F.quarter("fecha")
                )

                .withColumn(
                    "semana_anio",
                    F.weekofyear("fecha")
                )

                .withColumn(
                    "dia_semana_numero",
                    F.dayofweek("fecha")
                )

                # Spark:
                # 1 = domingo
                # 2 = lunes
                # ...
                # 7 = sábado

                .withColumn(
                    "nombre_dia",
                    F.when(
                        F.dayofweek("fecha") == 1,
                        "Domingo"
                    )
                    .when(
                        F.dayofweek("fecha") == 2,
                        "Lunes"
                    )
                    .when(
                        F.dayofweek("fecha") == 3,
                        "Martes"
                    )
                    .when(
                        F.dayofweek("fecha") == 4,
                        "Miércoles"
                    )
                    .when(
                        F.dayofweek("fecha") == 5,
                        "Jueves"
                    )
                    .when(
                        F.dayofweek("fecha") == 6,
                        "Viernes"
                    )
                    .when(
                        F.dayofweek("fecha") == 7,
                        "Sábado"
                    )
                )

                .withColumn(
                    "nombre_mes",
                    F.when(
                        F.month("fecha") == 1,
                        "Enero"
                    )
                    .when(
                        F.month("fecha") == 2,
                        "Febrero"
                    )
                    .when(
                        F.month("fecha") == 3,
                        "Marzo"
                    )
                    .when(
                        F.month("fecha") == 4,
                        "Abril"
                    )
                    .when(
                        F.month("fecha") == 5,
                        "Mayo"
                    )
                    .when(
                        F.month("fecha") == 6,
                        "Junio"
                    )
                    .when(
                        F.month("fecha") == 7,
                        "Julio"
                    )
                    .when(
                        F.month("fecha") == 8,
                        "Agosto"
                    )
                    .when(
                        F.month("fecha") == 9,
                        "Septiembre"
                    )
                    .when(
                        F.month("fecha") == 10,
                        "Octubre"
                    )
                    .when(
                        F.month("fecha") == 11,
                        "Noviembre"
                    )
                    .when(
                        F.month("fecha") == 12,
                        "Diciembre"
                    )
                )

                .withColumn(
                    "es_fin_semana",
                    F.dayofweek("fecha")
                    .isin(1, 7)
                )
            )

            # =================================================
            # IMPORTANTE
            #
            # Aquí seleccionamos exactamente las columnas que
            # ya existen en dim_fecha.
            #
            # Así evitamos alterar el esquema Gold.
            # =================================================

            columnas_gold = (
                dim_fecha.columns
            )

            nuevas_filas = (
                nuevas_filas
                .select(
                    *columnas_gold
                )
            )

            # =================================================
            # 6. INSERTAR
            # =================================================

            (
                nuevas_filas.write
                .format("delta")
                .mode("append")
                .save(GOLD_PATH)
            )

        # ====================================================
        # 7. VALIDACIONES
        # ====================================================

        print(
            "\nValidando dim_fecha..."
        )

        dim_final = (
            spark.read
            .format("delta")
            .load(GOLD_PATH)
        )

        # fecha_sk no puede repetirse

        duplicate_sk = (
            dim_final

            .groupBy("fecha_sk")

            .count()

            .filter(
                F.col("count") > 1
            )

            .count()
        )

        if duplicate_sk > 0:

            raise ValueError(
                "Se detectaron fecha_sk duplicadas."
            )

        # fecha tampoco puede repetirse

        duplicate_fecha = (
            dim_final

            .groupBy("fecha")

            .count()

            .filter(
                F.col("count") > 1
            )

            .count()
        )

        if duplicate_fecha > 0:

            raise ValueError(
                "Se detectaron fechas duplicadas "
                "en dim_fecha."
            )

        # Todas las fechas presentes en las facturas deben
        # poder encontrarse en Gold.

        fechas_sin_dimension = (
            fechas_silver.alias("s")

            .join(
                dim_final
                .select("fecha")
                .alias("g"),

                F.col("s.fecha")
                ==
                F.col("g.fecha"),

                "left_anti",
            )

            .count()
        )

        if fechas_sin_dimension > 0:

            raise ValueError(
                f"Existen {fechas_sin_dimension} fechas "
                "de facturas sin correspondencia "
                "en dim_fecha."
            )

        # ====================================================
        # 8. RESULTADO
        # ====================================================

        print("\n====================================")
        print("RESULTADO INCREMENTAL DIM_FECHA")
        print("====================================")

        print(
            f"Fechas nuevas insertadas: "
            f"{total_nuevas}"
        )

        print(
            f"Total filas dim_fecha: "
            f"{dim_final.count()}"
        )

        print(
            "\nÚltimas fechas:"
        )

        (
            dim_final

            .orderBy(
                F.col("fecha").desc()
            )

            .show(
                20,
                False
            )
        )

        print(
            "\ndim_fecha incremental "
            "procesada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    update_dim_fecha()