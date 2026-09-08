from delta.tables import DeltaTable

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import create_spark_session


SILVER_PATH = "/opt/spark-data/silver/vendedores"
GOLD_PATH = "/opt/spark-data/gold/dim_vendedor"


SCD1_COLUMNS = [
    "documento",
    "nombre",
    "apellidos",
    "correo",
    "fecha_de_contratacion",
    "activo",
]

SCD2_COLUMNS = [
    "sucursal_id",
]


def update_dim_vendedor():

    spark = create_spark_session(
        "gold_dim_vendedor_incremental"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD INCREMENTAL - DIM_VENDEDOR")
        print("====================================")

        # ====================================================
        # 1. LEER SILVER
        # ====================================================

        print("\nLeyendo vendedores Silver...")

        silver = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
        )

        # ====================================================
        # 2. LEER GOLD
        # ====================================================

        print("Leyendo dim_vendedor Gold...")

        gold = (
            spark.read
            .format("delta")
            .load(GOLD_PATH)
        )

        # ====================================================
        # 3. ÚLTIMA VERSIÓN DE CADA VENDEDOR EN SILVER
        #
        # vendedores usa snapshot_compare.
        # detected_at indica cuándo Silver detectó el estado.
        # ====================================================

        silver_window = (
            Window
            .partitionBy("vendedor_id")
            .orderBy(
                F.col("detected_at").desc()
            )
        )

        silver_latest = (
            silver

            .withColumn(
                "rn",
                F.row_number().over(
                    silver_window
                )
            )

            .filter(
                F.col("rn") == 1
            )

            .drop("rn")
        )

        # ====================================================
        # 4. VERSIÓN ACTUAL DE GOLD
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

                F.col("s.vendedor_id")
                ==
                F.col("g.vendedor_id"),

                "left",
            )

            .select(

                F.col("s.*"),

                F.col(
                    "g.vendedor_sk"
                ).alias(
                    "gold_vendedor_sk"
                ),

                F.col(
                    "g.sucursal_id"
                ).alias(
                    "g_sucursal_id"
                ),

                F.col(
                    "g.documento"
                ).alias(
                    "g_documento"
                ),

                F.col(
                    "g.nombre"
                ).alias(
                    "g_nombre"
                ),

                F.col(
                    "g.apellidos"
                ).alias(
                    "g_apellidos"
                ),

                F.col(
                    "g.correo"
                ).alias(
                    "g_correo"
                ),

                F.col(
                    "g.fecha_de_contratacion"
                ).alias(
                    "g_fecha_de_contratacion"
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

                # CORREGIDO:
                # En Gold la columna real se llama
                # source_updated_at.

                F.col(
                    "g.source_updated_at"
                ).alias(
                    "g_source_updated_at"
                ),
            )
        )

        # ====================================================
        # 6. VENDEDORES NUEVOS
        # ====================================================

        nuevos_vendedores = (
            comparacion

            .filter(
                F.col(
                    "gold_vendedor_sk"
                ).isNull()
            )
        )

        total_nuevos = (
            nuevos_vendedores.count()
        )

        print(
            f"\nVendedores nuevos: "
            f"{total_nuevos}"
        )

        # ====================================================
        # 7. CAMBIOS SCD2
        #
        # Si cambia sucursal_id:
        # cerramos versión anterior y creamos una nueva.
        # ====================================================

        cambios_scd2 = (
            comparacion

            .filter(
                F.col(
                    "gold_vendedor_sk"
                ).isNotNull()
            )

            .filter(
                ~F.col(
                    "sucursal_id"
                ).eqNullSafe(
                    F.col(
                        "g_sucursal_id"
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
        # documento
        # nombre
        # apellidos
        # correo
        # fecha_de_contratacion
        # activo
        #
        # En esta versión consideramos SCD1-only cuando
        # sucursal_id no cambió.
        # ====================================================

        cambios_scd1 = (
            comparacion

            .filter(
                F.col(
                    "gold_vendedor_sk"
                ).isNotNull()
            )

            .filter(
                F.col(
                    "sucursal_id"
                ).eqNullSafe(
                    F.col(
                        "g_sucursal_id"
                    )
                )
            )

            .filter(
                (
                    ~F.col(
                        "documento"
                    ).eqNullSafe(
                        F.col(
                            "g_documento"
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
                        "apellidos"
                    ).eqNullSafe(
                        F.col(
                            "g_apellidos"
                        )
                    )
                )
                |
                (
                    ~F.col(
                        "correo"
                    ).eqNullSafe(
                        F.col(
                            "g_correo"
                        )
                    )
                )
                |
                (
                    ~F.col(
                        "fecha_de_contratacion"
                    ).eqNullSafe(
                        F.col(
                            "g_fecha_de_contratacion"
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

        delta_vendedor = (
            DeltaTable.forPath(
                spark,
                GOLD_PATH,
            )
        )

        # ====================================================
        # 10. APLICAR CAMBIOS SCD1
        # ====================================================

        if total_scd1 > 0:

            print(
                "\nAplicando cambios SCD1..."
            )

            scd1_updates = (
                cambios_scd1

                .select(
                    "vendedor_id",
                    "documento",
                    "nombre",
                    "apellidos",
                    "correo",
                    "fecha_de_contratacion",
                    "activo",
                    "detected_at",
                )
            )

            (
                delta_vendedor.alias("g")

                .merge(
                    scd1_updates.alias("s"),

                    "g.vendedor_id = s.vendedor_id",
                )

                .whenMatchedUpdate(
                    set={
                        "documento":
                            "s.documento",

                        "nombre":
                            "s.nombre",

                        "apellidos":
                            "s.apellidos",

                        "correo":
                            "s.correo",

                        "fecha_de_contratacion":
                            "s.fecha_de_contratacion",

                        "activo":
                            "s.activo",

                        # CORREGIDO
                        "source_updated_at":
                            "s.detected_at",
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
                    "vendedor_id",
                    "detected_at",
                )
            )

            (
                delta_vendedor.alias("g")

                .merge(
                    scd2_close.alias("s"),

                    (
                        "g.vendedor_id = s.vendedor_id "
                        "AND g.es_actual = true"
                    ),
                )

                .whenMatchedUpdate(
                    set={
                        "fecha_fin":
                            "s.detected_at",

                        "es_actual":
                            "false",

                        # CORREGIDO
                        "source_updated_at":
                            "s.detected_at",
                    }
                )

                .execute()
            )

        # ====================================================
        # 12. OBTENER MAX VENDEDOR_SK
        # ====================================================

        max_vendedor_sk = (
            gold

            .agg(
                F.max(
                    "vendedor_sk"
                ).alias(
                    "max_sk"
                )
            )

            .collect()[0]["max_sk"]
        )

        if max_vendedor_sk is None:

            max_vendedor_sk = 0

        # ====================================================
        # 13. FILAS QUE DEBEN INSERTARSE
        #
        # - vendedor nuevo
        # - nueva versión SCD2
        # ====================================================

        filas_a_insertar = (
            nuevos_vendedores

            .withColumn(
                "tipo_insercion",

                F.lit(
                    "NUEVO_VENDEDOR"
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
                    "vendedor_id",
                    "detected_at",
                )
            )

            nuevas_filas = (
                filas_a_insertar

                # ------------------------------------------------
                # NUEVA SURROGATE KEY
                # ------------------------------------------------

                .withColumn(
                    "vendedor_sk",

                    (
                        F.lit(
                            max_vendedor_sk
                        )
                        +
                        F.row_number().over(
                            insert_window
                        )
                    )

                    .cast("long")
                )

                # ------------------------------------------------
                # FECHA INICIO
                #
                # Vendedor nuevo:
                # fecha_de_contratacion
                #
                # Nueva versión SCD2:
                # detected_at
                # ------------------------------------------------

                .withColumn(
                    "fecha_inicio",

                    F.when(
                        F.col(
                            "tipo_insercion"
                        )
                        ==
                        "NUEVO_VENDEDOR",

                        F.to_timestamp(
                            F.col(
                                "fecha_de_contratacion"
                            )
                        )
                    )

                    .otherwise(
                        F.col(
                            "detected_at"
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

                # ------------------------------------------------
                # CORREGIDO
                #
                # Silver:
                # detected_at
                #
                # Gold:
                # source_updated_at
                # ------------------------------------------------

                .withColumn(
                    "source_updated_at",

                    F.col(
                        "detected_at"
                    )
                )

                # ------------------------------------------------
                # ESQUEMA FINAL GOLD
                # ------------------------------------------------

                .select(
                    "vendedor_sk",
                    "vendedor_id",
                    "sucursal_id",
                    "documento",
                    "nombre",
                    "apellidos",
                    "correo",
                    "fecha_de_contratacion",
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
            "\nValidando dim_vendedor..."
        )

        dim_final = (
            spark.read
            .format("delta")
            .load(GOLD_PATH)
        )

        # ----------------------------------------------------
        # vendedor_sk DEBE SER ÚNICA
        # ----------------------------------------------------

        duplicate_sk = (
            dim_final

            .groupBy(
                "vendedor_sk"
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
                "Se detectaron vendedor_sk duplicados."
            )

        # ----------------------------------------------------
        # UNA SOLA VERSIÓN ACTUAL POR VENDEDOR
        # ----------------------------------------------------

        current_errors = (
            dim_final

            .filter(
                F.col(
                    "es_actual"
                ) == True
            )

            .groupBy(
                "vendedor_id"
            )

            .count()

            .filter(
                F.col(
                    "count"
                ) != 1
            )

            .count()
        )

        if current_errors > 0:

            raise ValueError(
                "Existen vendedores con una cantidad inválida "
                "de versiones actuales."
            )

        # ----------------------------------------------------
        # VALIDAR RANGOS TEMPORALES
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
                "en dim_vendedor."
            )

        # ====================================================
        # 16. RESULTADO
        # ====================================================

        print("\n====================================")
        print("RESULTADO INCREMENTAL DIM_VENDEDOR")
        print("====================================")

        print(
            f"Vendedores nuevos: "
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
            f"Total filas dim_vendedor: "
            f"{dim_final.count()}"
        )

        print(
            "\nÚltimas filas de dim_vendedor:"
        )

        (
            dim_final

            .orderBy(
                F.col(
                    "vendedor_sk"
                ).desc()
            )

            .show(
                30,
                False,
            )
        )

        print(
            "\ndim_vendedor incremental "
            "procesada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    update_dim_vendedor()