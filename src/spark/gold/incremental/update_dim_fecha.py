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
        # 2. LEER DIM_FECHA GOLD
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
        # 4. IDENTIFICAR FECHAS NUEVAS
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

        total_nuevas = nuevas_fechas.count()

        print(
            f"\nFechas nuevas encontradas: "
            f"{total_nuevas}"
        )

        # ====================================================
        # 5. CONSTRUIR NUEVAS FILAS
        #
        # IMPORTANTE:
        # Debe utilizar EXACTAMENTE el mismo esquema y lógica
        # que build_dim_fecha.py.
        # ====================================================

        if total_nuevas > 0:

            print(
                "\nConstruyendo nuevas fechas..."
            )

            nuevas_filas = (
                nuevas_fechas

                # --------------------------------------------
                # Surrogate key determinística YYYYMMDD
                # --------------------------------------------

                .withColumn(
                    "fecha_sk",
                    F.date_format(
                        "fecha",
                        "yyyyMMdd"
                    ).cast("int")
                )

                # --------------------------------------------
                # Atributos calendario
                # --------------------------------------------

                .withColumn(
                    "anio",
                    F.year("fecha")
                )

                .withColumn(
                    "trimestre",
                    F.quarter("fecha")
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
                    "dia_semana",
                    F.dayofweek("fecha")
                )
            )

            # =================================================
            # NOMBRE DEL MES
            # =================================================

            meses = F.create_map(
                F.lit(1), F.lit("ENERO"),
                F.lit(2), F.lit("FEBRERO"),
                F.lit(3), F.lit("MARZO"),
                F.lit(4), F.lit("ABRIL"),
                F.lit(5), F.lit("MAYO"),
                F.lit(6), F.lit("JUNIO"),
                F.lit(7), F.lit("JULIO"),
                F.lit(8), F.lit("AGOSTO"),
                F.lit(9), F.lit("SEPTIEMBRE"),
                F.lit(10), F.lit("OCTUBRE"),
                F.lit(11), F.lit("NOVIEMBRE"),
                F.lit(12), F.lit("DICIEMBRE"),
            )

            nuevas_filas = (
                nuevas_filas
                .withColumn(
                    "nombre_mes",
                    meses[
                        F.col("mes")
                    ]
                )
            )

            # =================================================
            # NOMBRE DEL DÍA
            # =================================================

            dias = F.create_map(
                F.lit(1), F.lit("DOMINGO"),
                F.lit(2), F.lit("LUNES"),
                F.lit(3), F.lit("MARTES"),
                F.lit(4), F.lit("MIERCOLES"),
                F.lit(5), F.lit("JUEVES"),
                F.lit(6), F.lit("VIERNES"),
                F.lit(7), F.lit("SABADO"),
            )

            nuevas_filas = (
                nuevas_filas
                .withColumn(
                    "nombre_dia",
                    dias[
                        F.col("dia_semana")
                    ]
                )
            )

            # =================================================
            # FIN DE SEMANA
            # =================================================

            nuevas_filas = (
                nuevas_filas
                .withColumn(
                    "es_fin_semana",
                    F.col(
                        "dia_semana"
                    ).isin(
                        1,
                        7,
                    )
                )
            )

            # =================================================
            # ESQUEMA FINAL
            #
            # Exactamente igual al initial load.
            # =================================================

            nuevas_filas = (
                nuevas_filas
                .select(
                    "fecha_sk",
                    "fecha",
                    "anio",
                    "trimestre",
                    "mes",
                    "nombre_mes",
                    "dia",
                    "dia_semana",
                    "nombre_dia",
                    "es_fin_semana",
                )
            )

            # =================================================
            # VALIDAR ESQUEMA CONTRA GOLD
            # =================================================

            if nuevas_filas.columns != dim_fecha.columns:

                raise ValueError(
                    "El esquema generado por "
                    "update_dim_fecha no coincide "
                    "con dim_fecha Gold.\n"
                    f"Nuevo: {nuevas_filas.columns}\n"
                    f"Gold:  {dim_fecha.columns}"
                )

            # =================================================
            # 6. INSERTAR
            # =================================================

            print(
                f"\nInsertando "
                f"{total_nuevas} fechas nuevas..."
            )

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

        # ----------------------------------------------------
        # fecha_sk única
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # fecha única
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Todas las fechas Silver deben existir en Gold
        # ----------------------------------------------------

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
                f"Existen "
                f"{fechas_sin_dimension} fechas "
                f"de facturas sin correspondencia "
                f"en dim_fecha."
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