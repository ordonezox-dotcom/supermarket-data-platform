from pyspark.sql import functions as F


def transform_clientes(df):

    return (
        df

        # Campos obligatorios
        .filter(
            F.col("cliente_id").isNotNull()
        )
        .filter(
            F.col("tipo_documento").isNotNull()
        )
        .filter(
            F.col("numero_documento").isNotNull()
        )
        .filter(
            F.col("nombre").isNotNull()
        )
        .filter(
            F.col("apellido").isNotNull()
        )
        .filter(
            F.col("updated_at").isNotNull()
        )

        # Normalización
        .withColumn(
            "tipo_documento",
            F.upper(
                F.trim(
                    F.col("tipo_documento")
                )
            )
        )

        .withColumn(
            "numero_documento",
            F.trim(
                F.col("numero_documento")
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
                    F.col("apellido")
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

        .withColumn(
            "telefono",
            F.trim(
                F.col("telefono")
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

        # Reglas de calidad
        .filter(
            F.length(
                F.col("numero_documento")
            ) > 0
        )

        .filter(
            F.length(
                F.col("nombre")
            ) > 0
        )

        .filter(
            F.length(
                F.col("apellido")
            ) > 0
        )

        .filter(
            F.col("correo").isNull()
            |
            F.col("correo").rlike(
                r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            )
        )
    )