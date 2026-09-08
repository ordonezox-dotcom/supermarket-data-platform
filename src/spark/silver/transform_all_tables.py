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

    # ========================================================
    # CARGA INCREMENTAL
    # ========================================================

    "clientes": {
        "strategy": "incremental",
        "cursor_column": "updated_at",
        "key_column": None,
        "transform": transform_clientes,
    },

    "productos": {
        "strategy": "incremental",
        "cursor_column": "updated_at",
        "key_column": None,
        "transform": transform_productos,
    },

    "inventario": {
        "strategy": "incremental",
        "cursor_column": "fecha_actualizacion",
        "key_column": None,
        "transform": transform_inventario,
    },

    "facturas": {
        "strategy": "incremental",
        "cursor_column": "factura_id",
        "key_column": None,
        "transform": transform_facturas,
    },

    "detalles_factura": {
        "strategy": "incremental",
        "cursor_column": "detalle_id",
        "key_column": None,
        "transform": transform_detalle_factura,
    },

    # ========================================================
    # FULL SNAPSHOT + CHANGE DETECTION
    # ========================================================

    "sucursales": {
        "strategy": "snapshot_compare",
        "cursor_column": None,
        "key_column": "sucursal_id",
        "transform": transform_sucursales,
    },

    "vendedores": {
        "strategy": "snapshot_compare",
        "cursor_column": None,
        "key_column": "vendedor_id",
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

                table_name=(
                    table_name
                ),

                strategy=(
                    config[
                        "strategy"
                    ]
                ),

                cursor_column=(
                    config[
                        "cursor_column"
                    ]
                ),

                key_column=(
                    config[
                        "key_column"
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