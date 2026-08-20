from pyspark.sql import functions as F


def transform_detalle_factura(df):

    return (
        df

        .filter(
            F.col("detalle_id").isNotNull()
        )

        .filter(
            F.col("factura_id").isNotNull()
        )

        .filter(
            F.col("producto_id").isNotNull()
        )

        .filter(
            F.col("cantidad") > 0
        )

        .filter(
            F.col(
                "precio_unitario"
            ) >= 0
        )

        .filter(
            F.col(
                "descuento_unitario"
            ) >= 0
        )

        .filter(
            F.col(
                "impuesto_unitario"
            ) >= 0
        )

        .filter(
            F.col(
                "total_linea"
            ) >= 0
        )
    )