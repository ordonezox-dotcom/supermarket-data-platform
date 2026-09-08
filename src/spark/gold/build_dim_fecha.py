from pyspark.sql import functions as F

from common import create_spark_session


SILVER_FACTURAS_PATH = "/opt/spark-data/silver/facturas"
GOLD_PATH = "/opt/spark-data/gold/dim_fecha"


def build_dim_fecha():

    spark = create_spark_session(
        "gold_dim_fecha"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD - DIM_FECHA")
        print("Generación dimensión calendario")
        print("====================================")

        # 1. LEER FACTURAS SILVER
        print("\nLeyendo fechas desde Silver facturas...")

        df_facturas = (
            spark.read
            .format("delta")
            .load(SILVER_FACTURAS_PATH)
        )

        # 2. OBTENER RANGO DE FECHAS
        rango = (
            df_facturas
            .select(
                F.min(
                    F.to_date("fecha_hora")
                ).alias("fecha_minima"),

                F.max(
                    F.to_date("fecha_hora")
                ).alias("fecha_maxima"),
            )
            .collect()[0]
        )

        fecha_minima = rango["fecha_minima"]
        fecha_maxima = rango["fecha_maxima"]

        if fecha_minima is None or fecha_maxima is None:
            raise ValueError(
                "No se encontraron fechas válidas en Silver facturas."
            )

        print(f"Fecha mínima encontrada: {fecha_minima}")
        print(f"Fecha máxima encontrada: {fecha_maxima}")

        # 3. GENERAR TODAS LAS FECHAS DEL RANGO
        df_fecha = (
            spark.range(1)
            .select(
                F.explode(
                    F.sequence(
                        F.lit(fecha_minima),
                        F.lit(fecha_maxima),
                        F.expr("INTERVAL 1 DAY"),
                    )
                ).alias("fecha")
            )
        )

        # 4. SURROGATE KEY DETERMINÍSTICA YYYYMMDD
        df_fecha = (
            df_fecha
            .withColumn(
                "fecha_sk",
                F.date_format(
                    "fecha",
                    "yyyyMMdd"
                ).cast("int")
            )
        )

        # 5. ATRIBUTOS DE CALENDARIO
        df_fecha = (
            df_fecha

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

        # 6. NOMBRE DEL MES
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

        df_fecha = (
            df_fecha
            .withColumn(
                "nombre_mes",
                meses[F.col("mes")]
            )
        )

        # 7. NOMBRE DEL DÍA
        dias = F.create_map(
            F.lit(1), F.lit("DOMINGO"),
            F.lit(2), F.lit("LUNES"),
            F.lit(3), F.lit("MARTES"),
            F.lit(4), F.lit("MIERCOLES"),
            F.lit(5), F.lit("JUEVES"),
            F.lit(6), F.lit("VIERNES"),
            F.lit(7), F.lit("SABADO"),
        )

        df_fecha = (
            df_fecha
            .withColumn(
                "nombre_dia",
                dias[F.col("dia_semana")]
            )
        )

        # 8. FIN DE SEMANA
        df_fecha = (
            df_fecha
            .withColumn(
                "es_fin_semana",
                F.col("dia_semana").isin(
                    1,
                    7,
                )
            )
        )

        # 9. ESQUEMA FINAL
        df_fecha = (
            df_fecha.select(
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

        # 10. GUARDAR GOLD
        print("\nGuardando dim_fecha...")

        (
            df_fecha.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true"
            )
            .save(GOLD_PATH)
        )

        total_fechas = df_fecha.count()

        print(f"Fechas creadas: {total_fechas}")

        # 11. MUESTRA
        print("\nPrimeras fechas:")

        (
            df_fecha
            .orderBy("fecha")
            .show(
                20,
                truncate=False,
            )
        )

        print("\nÚltimas fechas:")

        (
            df_fecha
            .orderBy(
                F.col("fecha").desc()
            )
            .show(
                10,
                truncate=False,
            )
        )

        print("\ndim_fecha creada correctamente.")

    finally:

        spark.stop()


if __name__ == "__main__":
    build_dim_fecha()