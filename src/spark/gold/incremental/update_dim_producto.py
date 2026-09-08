from delta.tables import DeltaTable

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import create_spark_session


SILVER_PATH = "/opt/spark-data/silver/productos"
GOLD_PATH = "/opt/spark-data/gold/dim_producto"

FIRST_VERSION_DATE = "1900-01-01 00:00:00"


SCD1_COLUMNS = [
    "codigo_barras",
    "nombre",
    "activo",
]

SCD2_COLUMNS = [
    "categoria",
    "subcategoria",
    "marca",
    "precio_venta",
    "costo_unitario",
]


def update_dim_producto():

    spark = create_spark_session(
        "gold_dim_producto_incremental"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD INCREMENTAL - DIM_PRODUCTO")
        print("====================================")

        # ====================================================
        # 1. LEER SILVER
        # ====================================================

        print("\nLeyendo productos Silver...")

        silver = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
        )

        # ====================================================
        # 2. LEER GOLD
        # ====================================================

        print("Leyendo dim_producto Gold...")

        gold = (
            spark.read
            .format("delta")
            .load(GOLD_PATH)
        )

        # ====================================================
        # 3. OBTENER ÚLTIMA VERSIÓN DE CADA PRODUCTO EN SILVER
        # ====================================================

        silver_window = (
            Window
            .partitionBy(
                "producto_id"
            )
            .orderBy(
                F.col(
                    "updated_at"
                ).desc()
            )
        )

        silver_latest = (
            silver

            .withColumn(
                "rn",
                F.row_number()
                .over(
                    silver_window
                )
            )

            .filter(
                F.col("rn") == 1
            )

            .drop("rn")
        )

        # ====================================================
        # 4. OBTENER VERSIÓN ACTUAL EN GOLD
        # ====================================================

        gold_actual = (
            gold
            .filter(
                F.col("es_actual") == True
            )
        )

        # ====================================================
        # 5. COMPARAR SILVER VS GOLD
        # ====================================================

        comparacion = (
            silver_latest.alias("s")

            .join(
                gold_actual.alias("g"),

                F.col(
                    "s.producto_id"
                )
                ==
                F.col(
                    "g.producto_id"
                ),

                "left",
            )

            .select(

                F.col("s.*"),

                F.col(
                    "g.producto_sk"
                ).alias(
                    "gold_producto_sk"
                ),

                F.col(
                    "g.codigo_barras"
                ).alias(
                    "g_codigo_barras"
                ),

                F.col(
                    "g.nombre"
                ).alias(
                    "g_nombre"
                ),

                F.col(
                    "g.categoria"
                ).alias(
                    "g_categoria"
                ),

                F.col(
                    "g.subcategoria"
                ).alias(
                    "g_subcategoria"
                ),

                F.col(
                    "g.marca"
                ).alias(
                    "g_marca"
                ),

                F.col(
                    "g.precio_venta"
                ).alias(
                    "g_precio_venta"
                ),

                F.col(
                    "g.costo_unitario"
                ).alias(
                    "g_costo_unitario"
                ),

                F.col(
                    "g.activo"
                ).alias(
                    "g_activo"
                ),

                F.col(
                    "g.fecha_inicio"
                ).alias(
                    "g_fecha_inicio"
                ),

                F.col(
                    "g.source_updated_at"
                ).alias(
                    "g_source_updated_at"
                ),
            )
        )

        # ====================================================
        # 6. PRODUCTOS NUEVOS
        # ====================================================

        nuevos_productos = (
            comparacion
            .filter(
                F.col(
                    "gold_producto_sk"
                ).isNull()
            )
        )

        total_nuevos = (
            nuevos_productos.count()
        )

        print(
            f"\nProductos nuevos: "
            f"{total_nuevos}"
        )

        # ====================================================
        # 7. CAMBIOS SCD2
        # ====================================================

        cambios_scd2 = (
            comparacion

            .filter(
                F.col(
                    "gold_producto_sk"
                ).isNotNull()
            )

            .filter(
                (
                    ~F.col(
                        "categoria"
                    ).eqNullSafe(
                        F.col(
                            "g_categoria"
                        )
                    )
                )
                |
                (
                    ~F.col(
                        "subcategoria"
                    ).eqNullSafe(
                        F.col(
                            "g_subcategoria"
                        )
                    )
                )
                |
                (
                    ~F.col(
                        "marca"
                    ).eqNullSafe(
                        F.col(
                            "g_marca"
                        )
                    )
                )
                |
                (
                    ~F.col(
                        "precio_venta"
                    ).eqNullSafe(
                        F.col(
                            "g_precio_venta"
                        )
                    )
                )
                |
                (
                    ~F.col(
                        "costo_unitario"
                    ).eqNullSafe(
                        F.col(
                            "g_costo_unitario"
                        )
                    )
                )
            )
        )

        total_scd2 = (
            cambios_scd2.count()
        )

        print(
            f"Cambios SCD2: "
            f"{total_scd2}"
        )

        # ====================================================
        # 8. CAMBIOS SCD1
        #
        # Solo cuando NO hubo cambio SCD2.
        # ====================================================

        cambios_scd1 = (
            comparacion

            .filter(
                F.col(
                    "gold_producto_sk"
                ).isNotNull()
            )

            .filter(
                F.col(
                    "categoria"
                ).eqNullSafe(
                    F.col(
                        "g_categoria"
                    )
                )
                &
                F.col(
                    "subcategoria"
                ).eqNullSafe(
                    F.col(
                        "g_subcategoria"
                    )
                )
                &
                F.col(
                    "marca"
                ).eqNullSafe(
                    F.col(
                        "g_marca"
                    )
                )
                &
                F.col(
                    "precio_venta"
                ).eqNullSafe(
                    F.col(
                        "g_precio_venta"
                    )
                )
                &
                F.col(
                    "costo_unitario"
                ).eqNullSafe(
                    F.col(
                        "g_costo_unitario"
                    )
                )
            )

            .filter(
                (
                    ~F.col(
                        "codigo_barras"
                    ).eqNullSafe(
                        F.col(
                            "g_codigo_barras"
                        )
                    )
                )
                |
                (
                    ~F.col(
                        "nombre"
                    ).eqNullSafe(
                        F.col(
                            "g_nombre"
                        )
                    )
                )
                |
                (
                    ~F.col(
                        "activo"
                    ).eqNullSafe(
                        F.col(
                            "g_activo"
                        )
                    )
                )
            )
        )

        total_scd1 = (
            cambios_scd1.count()
        )

        print(
            f"Cambios solo SCD1: "
            f"{total_scd1}"
        )

        # ====================================================
        # 9. ABRIR DELTA TABLE
        # ====================================================

        delta_producto = (
            DeltaTable.forPath(
                spark,
                GOLD_PATH,
            )
        )

        # ====================================================
        # 10. APLICAR SCD1
        #
        # Sobrescribimos atributos SCD1 en todas las versiones
        # históricas del producto.
        # ====================================================

        if total_scd1 > 0:

            print(
                "\nAplicando cambios SCD1..."
            )

            scd1_updates = (
                cambios_scd1
                .select(
                    "producto_id",
                    "codigo_barras",
                    "nombre",
                    "activo",
                    "updated_at",
                )
            )

            (
                delta_producto.alias("g")

                .merge(
                    scd1_updates.alias("s"),

                    "g.producto_id = s.producto_id",
                )

                .whenMatchedUpdate(
                    set={
                        "codigo_barras":
                            "s.codigo_barras",

                        "nombre":
                            "s.nombre",

                        "activo":
                            "s.activo",

                        "source_updated_at":
                            "s.updated_at",
                    }
                )

                .execute()
            )

        # ====================================================
        # 11. CERRAR VERSIONES ACTUALES SCD2
        # ====================================================

        if total_scd2 > 0:

            print(
                "\nCerrando versiones SCD2..."
            )

            scd2_close = (
                cambios_scd2
                .select(
                    "producto_id",
                    "updated_at",
                )
            )

            (
                delta_producto.alias("g")

                .merge(
                    scd2_close.alias("s"),

                    (
                        "g.producto_id = s.producto_id "
                        "AND g.es_actual = true"
                    ),
                )

                .whenMatchedUpdate(
                    set={
                        "fecha_fin":
                            "s.updated_at",

                        "es_actual":
                            "false",

                        "source_updated_at":
                            "s.updated_at",
                    }
                )

                .execute()
            )

        # ====================================================
        # 12. OBTENER MAX PRODUCTO_SK
        # ====================================================

        max_producto_sk = (
            gold

            .agg(
                F.max(
                    "producto_sk"
                ).alias(
                    "max_sk"
                )
            )

            .collect()[0]["max_sk"]
        )

        if max_producto_sk is None:

            max_producto_sk = 0

        # ====================================================
        # 13. PREPARAR FILAS NUEVAS
        #
        # Incluye:
        #
        # - producto totalmente nuevo
        # - nueva versión SCD2
        # ====================================================

        filas_a_insertar = (
            nuevos_productos

            .withColumn(
                "tipo_insercion",
                F.lit(
                    "NUEVO_PRODUCTO"
                )
            )

            .unionByName(
                cambios_scd2

                .withColumn(
                    "tipo_insercion",
                    F.lit(
                        "NUEVA_VERSION_SCD2"
                    )
                ),

                allowMissingColumns=True,
            )
        )

        total_insertar = (
            filas_a_insertar.count()
        )

        # ====================================================
        # 14. INSERTAR NUEVAS VERSIONES
        # ====================================================

        if total_insertar > 0:

            print(
                f"\nFilas nuevas a insertar: "
                f"{total_insertar}"
            )

            insert_window = (
                Window.orderBy(
                    "producto_id",
                    "updated_at",
                )
            )

            nuevas_filas = (
                filas_a_insertar

                .withColumn(
                    "producto_sk",

                    (
                        F.lit(
                            max_producto_sk
                        )
                        +
                        F.row_number()
                        .over(
                            insert_window
                        )
                    )
                    .cast("long")
                )

                .withColumn(
                    "fecha_inicio",

                    F.when(
                        F.col(
                            "tipo_insercion"
                        )
                        ==
                        "NUEVO_PRODUCTO",

                        F.to_timestamp(
                            F.lit(
                                FIRST_VERSION_DATE
                            )
                        )
                    )

                    .otherwise(
                        F.col(
                            "updated_at"
                        )
                    )
                )

                .withColumn(
                    "fecha_fin",

                    F.lit(
                        None
                    ).cast(
                        "timestamp"
                    )
                )

                .withColumn(
                    "es_actual",
                    F.lit(
                        True
                    )
                )

                .withColumn(
                    "source_updated_at",
                    F.col(
                        "updated_at"
                    )
                )

                .select(
                    "producto_sk",
                    "producto_id",
                    "codigo_barras",
                    "nombre",
                    "categoria",
                    "subcategoria",
                    "marca",
                    "precio_venta",
                    "costo_unitario",
                    "activo",
                    "fecha_inicio",
                    "fecha_fin",
                    "es_actual",
                    "source_updated_at",
                )
            )

            (
                nuevas_filas.write
                .format("delta")
                .mode("append")
                .save(
                    GOLD_PATH
                )
            )

        # ====================================================
        # 15. VALIDACIONES POST-CARGA
        # ====================================================

        print(
            "\nValidando dim_producto..."
        )

        dim_final = (
            spark.read
            .format("delta")
            .load(GOLD_PATH)
        )

        # ----------------------------------------------------
        # producto_sk debe ser único
        # ----------------------------------------------------

        duplicate_sk = (
            dim_final

            .groupBy(
                "producto_sk"
            )

            .count()

            .filter(
                F.col(
                    "count"
                ) > 1
            )

            .count()
        )

        if duplicate_sk > 0:

            raise ValueError(
                "Se detectaron producto_sk duplicados."
            )

        # ----------------------------------------------------
        # Cada producto debe tener una única versión actual
        # ----------------------------------------------------

        current_errors = (
            dim_final

            .filter(
                F.col(
                    "es_actual"
                )
                ==
                True
            )

            .groupBy(
                "producto_id"
            )

            .count()

            .filter(
                F.col(
                    "count"
                )
                != 1
            )

            .count()
        )

        if current_errors > 0:

            raise ValueError(
                "Existen productos con una cantidad inválida "
                "de versiones actuales."
            )

        # ----------------------------------------------------
        # Rangos SCD2 válidos
        # ----------------------------------------------------

        temporal_errors = (
            dim_final

            .filter(
                F.col(
                    "fecha_inicio"
                ).isNull()
                |
                (
                    F.col(
                        "fecha_fin"
                    ).isNotNull()
                    &
                    (
                        F.col(
                            "fecha_inicio"
                        )
                        >=
                        F.col(
                            "fecha_fin"
                        )
                    )
                )
            )

            .count()
        )

        if temporal_errors > 0:

            raise ValueError(
                "Se detectaron rangos temporales inválidos "
                "en dim_producto."
            )

        # ====================================================
        # 16. RESULTADO
        # ====================================================

        print("\n====================================")
        print("RESULTADO INCREMENTAL DIM_PRODUCTO")
        print("====================================")

        print(
            f"Productos nuevos: "
            f"{total_nuevos}"
        )

        print(
            f"Cambios SCD1: "
            f"{total_scd1}"
        )

        print(
            f"Cambios SCD2: "
            f"{total_scd2}"
        )

        print(
            f"Nuevas filas insertadas: "
            f"{total_insertar}"
        )

        print(
            "\nÚltimas filas de dim_producto:"
        )

        (
            dim_final

            .orderBy(
                F.col(
                    "producto_sk"
                ).desc()
            )

            .show(
                30,
                False,
            )
        )

        print(
            "\ndim_producto incremental "
            "procesada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    update_dim_producto()