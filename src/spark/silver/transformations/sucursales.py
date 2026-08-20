from pyspark.sql import functions as F


def transform_sucursales(df):

    return (
        df

        .filter(
            F.col("sucursal_id").isNotNull()
        )

        .filter(
            F.col("nombre").isNotNull()
        )

        .filter(
            F.col("ciudad").isNotNull()
        )

        .withColumn(
            "nombre",
            F.initcap(
                F.trim(
                    F.col("nombre")
                )
            )
        )

        .withColumn(
            "ciudad",
            F.initcap(
                F.trim(
                    F.col("ciudad")
                )
            )
        )

        .withColumn(
            "direccion",
            F.trim(
                F.col("direccion")
            )
        )
    )