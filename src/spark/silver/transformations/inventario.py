from pyspark.sql import functions as F


def transform_inventario(df):

    return (
        df

        .filter(
            F.col("sucursal_id").isNotNull()
        )

        .filter(
            F.col("producto_id").isNotNull()
        )

        .filter(
            F.col(
                "fecha_actualizacion"
            ).isNotNull()
        )

        .filter(
            F.col(
                "cantidad_disponible"
            ) >= 0
        )

        .filter(
            F.col(
                "stock_minimo"
            ) >= 0
        )

        .filter(
            F.col(
                "stock_maximo"
            ) >= 0
        )

        .filter(
            F.col("stock_maximo")
            >=
            F.col("stock_minimo")
        )
    )