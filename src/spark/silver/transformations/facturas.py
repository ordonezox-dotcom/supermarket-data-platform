from pyspark.sql import functions as F


def transform_facturas(df):

    return (
        df

        .filter(
            F.col("factura_id").isNotNull()
        )

        .filter(
            F.col("numero_factura").isNotNull()
        )

        .filter(
            F.col("sucursal_id").isNotNull()
        )

        .filter(
            F.col("vendedor_id").isNotNull()
        )

        .filter(
            F.col("fecha_hora").isNotNull()
        )

        .withColumn(
            "numero_factura",
            F.upper(
                F.trim(
                    F.col("numero_factura")
                )
            )
        )

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

        .filter(
            F.col("subtotal") >= 0
        )

        .filter(
            F.col(
                "descuento_total"
            ) >= 0
        )

        .filter(
            F.col(
                "impuesto_total"
            ) >= 0
        )

        .filter(
            F.col("total") >= 0
        )
    )