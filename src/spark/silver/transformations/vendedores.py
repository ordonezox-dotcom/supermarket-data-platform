from pyspark.sql import functions as F


def transform_vendedores(df):

    return (
        df

        .filter(
            F.col("vendedor_id").isNotNull()
        )

        .filter(
            F.col("sucursal_id").isNotNull()
        )

        .filter(
            F.col("documento").isNotNull()
        )

        .filter(
            F.col("nombre").isNotNull()
        )

        .filter(
            F.col("apellidos").isNotNull()
        )

        .withColumn(
            "documento",
            F.trim(
                F.col("documento")
            )
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
            "apellido",
            F.initcap(
                F.trim(
                    F.col("apellidos")
                )
            )
        )

        .withColumn(
            "correo",
            F.lower(
                F.trim(
                    F.col("correo")
                )
            )
        )
    )