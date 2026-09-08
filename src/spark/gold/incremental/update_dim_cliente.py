from delta.tables import DeltaTable

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import create_spark_session


SILVER_PATH = "/opt/spark-data/silver/clientes"
GOLD_PATH = "/opt/spark-data/gold/dim_cliente"

UNKNOWN_CLIENTE_SK = 0


# ============================================================
# COLUMNAS SCD
# ============================================================

SCD1_COLUMNS = [
    "tipo_documento",
    "numero_documento",
    "nombre",
    "apellido",
    "correo",
    "telefono",
    "fecha_nacimiento",
    "activo",
]

SCD2_COLUMNS = [
    "ciudad",
]


def update_dim_cliente():

    spark = create_spark_session(
        "gold_dim_cliente_incremental"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD INCREMENTAL - DIM_CLIENTE")
        print("====================================")

        # ====================================================
        # 1. LEER SILVER
        # ====================================================

        print("\nLeyendo clientes Silver...")

        silver = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
        )

        # ====================================================
        # 2. LEER GOLD
        # ====================================================

        print("Leyendo dim_cliente Gold...")

        gold = (
            spark.read
            .format("delta")
            .load(GOLD_PATH)
        )

        # ====================================================
        # 3. VALIDAR CLIENTE DESCONOCIDO
        # ====================================================

        unknown_count = (
            gold
            .filter(
                F.col("cliente_sk")
                ==
                UNKNOWN_CLIENTE_SK
            )
            .count()
        )

        if unknown_count != 1:

            raise ValueError(
                "dim_cliente debe contener exactamente "
                "un cliente desconocido con cliente_sk = 0."
            )

        # ====================================================
        # 4. OBTENER ÚLTIMA VERSIÓN DE CADA CLIENTE EN SILVER
        #
        # Silver puede contener historial.
        # Para el incremental primero necesitamos identificar
        # la versión más reciente recibida de cada cliente.
        # ====================================================

        silver_window = (
            Window
            .partitionBy(
                "cliente_id"
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
        # 5. OBTENER VERSIÓN ACTUAL DE CADA CLIENTE EN GOLD
        # ====================================================

        gold_actual = (
            gold

            .filter(
                (
                    F.col(
                        "es_actual"
                    )
                    ==
                    True
                )
                &
                (
                    F.col(
                        "cliente_sk"
                    )
                    !=
                    UNKNOWN_CLIENTE_SK
                )
            )
        )

        # ====================================================
        # 6. COMPARAR SILVER VS GOLD
        # ====================================================

        comparacion = (
            silver_latest.alias("s")

            .join(
                gold_actual.alias("g"),

                F.col(
                    "s.cliente_id"
                )
                ==
                F.col(
                    "g.cliente_id"
                ),

                "left",
            )

            .select(

                F.col(
                    "s.*"
                ),

                F.col(
                    "g.cliente_sk"
                ).alias(
                    "gold_cliente_sk"
                ),

                F.col(
                    "g.tipo_documento"
                ).alias(
                    "g_tipo_documento"
                ),

                F.col(
                    "g.numero_documento"
                ).alias(
                    "g_numero_documento"
                ),

                F.col(
                    "g.nombre"
                ).alias(
                    "g_nombre"
                ),

                F.col(
                    "g.apellido"
                ).alias(
                    "g_apellido"
                ),

                F.col(
                    "g.correo"
                ).alias(
                    "g_correo"
                ),

                F.col(
                    "g.telefono"
                ).alias(
                    "g_telefono"
                ),

                F.col(
                    "g.ciudad"
                ).alias(
                    "g_ciudad"
                ),

                F.col(
                    "g.fecha_nacimiento"
                ).alias(
                    "g_fecha_nacimiento"
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
        # 7. IDENTIFICAR CLIENTES NUEVOS
        # ====================================================

        nuevos_clientes = (
            comparacion

            .filter(
                F.col(
                    "gold_cliente_sk"
                ).isNull()
            )
        )

        total_nuevos = (
            nuevos_clientes.count()
        )

        print(
            f"\nClientes nuevos: "
            f"{total_nuevos}"
        )

        # ====================================================
        # 8. DETECTAR CAMBIOS SCD2
        #
        # Actualmente:
        # ciudad = SCD2
        # ====================================================

        cambios_scd2 = (
            comparacion

            .filter(
                F.col(
                    "gold_cliente_sk"
                ).isNotNull()
                &
                (
                    ~F.col(
                        "ciudad"
                    ).eqNullSafe(
                        F.col(
                            "g_ciudad"
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
        # 9. DETECTAR CAMBIOS SCD1
        #
        # Solo consideramos aquí clientes sin cambio SCD2.
        # Si existe un cambio SCD2, la nueva versión ya llevará
        # también los valores SCD1 más recientes.
        # ====================================================

        cambios_scd1 = (
            comparacion

            .filter(
                F.col(
                    "gold_cliente_sk"
                ).isNotNull()
            )

            .filter(
                F.col(
                    "ciudad"
                ).eqNullSafe(
                    F.col(
                        "g_ciudad"
                    )
                )
            )

            .filter(
                (
                    ~F.col(
                        "tipo_documento"
                    ).eqNullSafe(
                        F.col(
                            "g_tipo_documento"
                        )
                    )
                )
                |
                (
                    ~F.col(
                        "numero_documento"
                    ).eqNullSafe(
                        F.col(
                            "g_numero_documento"
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
                        "apellido"
                    ).eqNullSafe(
                        F.col(
                            "g_apellido"
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
                        "telefono"
                    ).eqNullSafe(
                        F.col(
                            "g_telefono"
                        )
                    )
                )
                |
                (
                    ~F.col(
                        "fecha_nacimiento"
                    ).eqNullSafe(
                        F.col(
                            "g_fecha_nacimiento"
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
        # 10. ABRIR DELTA TABLE
        # ====================================================

        delta_cliente = (
            DeltaTable.forPath(
                spark,
                GOLD_PATH,
            )
        )

        # ====================================================
        # 11. ACTUALIZAR CAMBIOS SCD1
        #
        # Importante:
        # SCD1 sobrescribe los atributos sin crear nueva SK.
        #
        # En este diseño actualizamos todas las versiones
        # históricas de ese cliente para que los atributos
        # SCD1 representen siempre el valor más reciente.
        # ====================================================

        if total_scd1 > 0:

            print(
                "\nAplicando actualizaciones SCD1..."
            )

            scd1_updates = (
                cambios_scd1

                .select(
                    "cliente_id",
                    "tipo_documento",
                    "numero_documento",
                    "nombre",
                    "apellido",
                    "correo",
                    "telefono",
                    "fecha_nacimiento",
                    "activo",
                    "updated_at",
                )
            )

            (
                delta_cliente.alias("g")

                .merge(
                    scd1_updates.alias("s"),

                    (
                        "g.cliente_id = s.cliente_id "
                        "AND g.cliente_sk <> 0"
                    ),
                )

                .whenMatchedUpdate(
                    set={
                        "tipo_documento":
                            "s.tipo_documento",

                        "numero_documento":
                            "s.numero_documento",

                        "nombre":
                            "s.nombre",

                        "apellido":
                            "s.apellido",

                        "correo":
                            "s.correo",

                        "telefono":
                            "s.telefono",

                        "fecha_nacimiento":
                            "s.fecha_nacimiento",

                        "activo":
                            "s.activo",

                        "source_updated_at":
                            "s.updated_at",
                    }
                )

                .execute()
            )

        # ====================================================
        # 12. PROCESAR CAMBIOS SCD2
        #
        # Primero cerramos la versión actual.
        # ====================================================

        if total_scd2 > 0:

            print(
                "\nCerrando versiones SCD2 actuales..."
            )

            scd2_close = (
                cambios_scd2

                .select(
                    "cliente_id",
                    "updated_at",
                )
            )

            (
                delta_cliente.alias("g")

                .merge(
                    scd2_close.alias("s"),

                    (
                        "g.cliente_id = s.cliente_id "
                        "AND g.es_actual = true "
                        "AND g.cliente_sk <> 0"
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
        # 13. CALCULAR NUEVAS SURROGATE KEYS
        #
        # Nuevos clientes + nuevas versiones SCD2 necesitan SK.
        # ====================================================

        max_cliente_sk = (
            gold

            .agg(
                F.max(
                    "cliente_sk"
                ).alias(
                    "max_sk"
                )
            )

            .collect()[0]["max_sk"]
        )

        if max_cliente_sk is None:

            max_cliente_sk = 0

        filas_a_insertar = (
            nuevos_clientes
            .withColumn(
                "tipo_insercion",
                F.lit(
                    "NUEVO_CLIENTE"
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
        # 14. INSERTAR NUEVAS FILAS
        # ====================================================

        if total_insertar > 0:

            print(
                f"\nFilas nuevas a insertar: "
                f"{total_insertar}"
            )

            insert_window = (
                Window.orderBy(
                    "cliente_id",
                    "updated_at",
                )
            )

            nuevas_filas = (
                filas_a_insertar

                .withColumn(
                    "cliente_sk",

                    (
                        F.lit(
                            max_cliente_sk
                        )
                        +
                        F.row_number()
                        .over(
                            insert_window
                        )
                    )
                    .cast("long")
                )

                # Fecha_inicio:
                #
                # cliente nuevo:
                # fecha_registro
                #
                # nueva versión SCD2:
                # updated_at

                .withColumn(
                    "fecha_inicio",

                    F.when(
                        F.col(
                            "tipo_insercion"
                        )
                        ==
                        "NUEVO_CLIENTE",

                        F.col(
                            "fecha_registro"
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
                    "cliente_sk",
                    "cliente_id",
                    "tipo_documento",
                    "numero_documento",
                    "nombre",
                    "apellido",
                    "correo",
                    "telefono",
                    "ciudad",
                    "fecha_nacimiento",
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
            "\nValidando dim_cliente después "
            "del incremental..."
        )

        dim_final = (
            spark.read
            .format("delta")
            .load(GOLD_PATH)
        )

        # ----------------------------------------------------
        # SK = 0 debe existir exactamente una vez
        # ----------------------------------------------------

        unknown_final = (
            dim_final
            .filter(
                F.col(
                    "cliente_sk"
                )
                ==
                UNKNOWN_CLIENTE_SK
            )
            .count()
        )

        if unknown_final != 1:

            raise ValueError(
                "Error en miembro desconocido de dim_cliente."
            )

        # ----------------------------------------------------
        # cliente_sk debe ser único
        # ----------------------------------------------------

        duplicated_sk = (
            dim_final

            .groupBy(
                "cliente_sk"
            )

            .count()

            .filter(
                F.col(
                    "count"
                ) > 1
            )

            .count()
        )

        if duplicated_sk > 0:

            raise ValueError(
                "Se detectaron cliente_sk duplicados."
            )

        # ----------------------------------------------------
        # Cada cliente real debe tener una sola versión actual
        # ----------------------------------------------------

        current_errors = (
            dim_final

            .filter(
                F.col(
                    "cliente_id"
                ).isNotNull()
            )

            .filter(
                F.col(
                    "es_actual"
                )
                ==
                True
            )

            .groupBy(
                "cliente_id"
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
                "Existen clientes con una cantidad inválida "
                "de versiones actuales."
            )

        # ----------------------------------------------------
        # Validar rangos temporales
        # ----------------------------------------------------

        temporal_errors = (
            dim_final

            .filter(
                F.col(
                    "cliente_id"
                ).isNotNull()
            )

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
                "Se detectaron rangos temporales "
                "inválidos en dim_cliente."
            )

        # ====================================================
        # 16. RESULTADO
        # ====================================================

        print("\n====================================")
        print("RESULTADO INCREMENTAL DIM_CLIENTE")
        print("====================================")

        print(
            f"Clientes nuevos: "
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
            "\nCliente desconocido:"
        )

        (
            dim_final

            .filter(
                F.col(
                    "cliente_sk"
                )
                ==
                UNKNOWN_CLIENTE_SK
            )

            .show(
                truncate=False
            )
        )

        print(
            "\nÚltimas filas de dim_cliente:"
        )

        (
            dim_final

            .orderBy(
                F.col(
                    "cliente_sk"
                ).desc()
            )

            .show(
                30,
                False,
            )
        )

        print(
            "\ndim_cliente incremental "
            "procesada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    update_dim_cliente()