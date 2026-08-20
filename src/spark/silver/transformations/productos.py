from pyspark.sql import functions as F


def transform_productos(df):

    return (
        df

        .filter(
            F.col("producto_id").isNotNull()
        )

        .filter(
            F.col("codigo_barras").isNotNull()
        )

        .filter(
            F.col("nombre").isNotNull()
        )

        .filter(
            F.col("updated_at").isNotNull()
        )

        .withColumn(
            "codigo_barras",
            F.trim(
                F.col("codigo_barras")
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
            "categoria",
            F.initcap(
                F.trim(
                    F.col("categoria")
                )
            )
        )

        .withColumn(
            "subcategoria",
            F.initcap(
                F.trim(
                    F.col("subcategoria")
                )
            )
        )

        .withColumn(
            "marca",
            F.initcap(
                F.trim(
                    F.col("marca")
                )
            )
        )

        .filter(
            F.col("precio_venta") >= 0
        )

        .filter(
            F.col("costo_unitario") >= 0
        )
    )