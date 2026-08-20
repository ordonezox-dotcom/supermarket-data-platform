from pyspark.sql import SparkSession

from common import process_table

from transformations.clientes import (
    transform_clientes,
)

from transformations.productos import (
    transform_productos,
)

from transformations.inventario import (
    transform_inventario,
)

from transformations.facturas import (
    transform_facturas,
)

from transformations.detalle_factura import (
    transform_detalle_factura,
)

from transformations.sucursales import (
    transform_sucursales,
)

from transformations.vendedores import (
    transform_vendedores,
)


TABLE_CONFIG = {
    "clientes": {
        "cursor_column": "updated_at",
        "transform": transform_clientes,
    },

    "productos": {
        "cursor_column": "updated_at",
        "transform": transform_productos,
    },

    "inventario": {
        "cursor_column": "fecha_actualizacion",
        "transform": transform_inventario,
    },

    "facturas": {
        "cursor_column": "factura_id",
        "transform": transform_facturas,
    },

    "detalles_factura": {
        "cursor_column": "detalle_id",
        "transform": transform_detalle_factura,
    },

    "sucursales": {
        "cursor_column": "sucursal_id",
        "transform": transform_sucursales,
    },

    "vendedores": {
        "cursor_column": "vendedor_id",
        "transform": transform_vendedores,
    },
}


def create_spark_session():

    return (
        SparkSession.builder
        .appName(
            "silver_all_tables"
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def transform_all_tables():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:

        print(
            "\n===================================="
        )

        print(
            "BRONZE -> SILVER"
        )

        print(
            "===================================="
        )

        for table_name, config in (
            TABLE_CONFIG.items()
        ):

            process_table(
                spark=spark,
                table_name=table_name,
                cursor_column=(
                    config[
                        "cursor_column"
                    ]
                ),
                transform_function=(
                    config[
                        "transform"
                    ]
                ),
            )

    finally:

        spark.stop()


if __name__ == "__main__":

    transform_all_tables()