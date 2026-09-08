from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import create_spark_session


SILVER_PATH = "/opt/spark-data/silver/clientes"
GOLD_PATH = "/opt/spark-data/gold/dim_cliente"


def build_dim_cliente_initial():

    spark = create_spark_session(
        "gold_dim_cliente_initial"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD - DIM_CLIENTE")
        print("Carga inicial SCD")
        print("====================================")

        # ====================================================
        # 1. LEER SILVER
        # ====================================================

        print("\nLeyendo Silver clientes...")

        df_silver = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
        )

        print(
            f"Registros encontrados: "
            f"{df_silver.count()}"
        )

        # ====================================================
        # 2. ORDENAR TODAS LAS VERSIONES DE CADA CLIENTE
        #
        # updated_at permite reconstruir el orden en el que
        # fueron apareciendo las versiones dentro del sistema
        # digital.
        # ====================================================

        version_window = (
            Window
            .partitionBy("cliente_id")
            .orderBy("updated_at")
        )

        # ====================================================
        # 3. DETECTAR CAMBIOS SCD2
        #
        # ciudad = atributo SCD2
        #
        # Conservamos:
        #
        # - la primera versión conocida del cliente
        # - cualquier fila posterior donde cambió ciudad
        #
        # row_number permite detectar correctamente la primera
        # versión incluso si ciudad pudiera ser NULL.
        # ====================================================

        df_versions = (
            df_silver

            .withColumn(
                "_numero_fila_source",
                F.row_number().over(
                    version_window
                )
            )

            .withColumn(
                "ciudad_anterior",
                F.lag(
                    "ciudad"
                ).over(
                    version_window
                )
            )

            .withColumn(
                "es_primera_version",
                F.col(
                    "_numero_fila_source"
                ) == 1
            )

            .withColumn(
                "cambio_scd2",
                (
                    F.col(
                        "es_primera_version"
                    )
                    |
                    ~F.col(
                        "ciudad"
                    ).eqNullSafe(
                        F.col(
                            "ciudad_anterior"
                        )
                    )
                )
            )
        )

        # ====================================================
        # 4. CONSERVAR SOLO VERSIONES SCD2 REALES
        #
        # Los cambios únicamente SCD1 no crean una nueva fila
        # histórica en la dimensión.
        # ====================================================

        df_scd2 = (
            df_versions
            .filter(
                F.col("cambio_scd2")
            )
        )

        # ====================================================
        # 5. NUMERAR LAS VERSIONES SCD2
        #
        # Esta numeración ocurre DESPUÉS de eliminar los
        # cambios que pertenecen únicamente a atributos SCD1.
        # ====================================================

        scd_window = (
            Window
            .partitionBy("cliente_id")
            .orderBy("updated_at")
        )

        df_scd2 = (
            df_scd2

            .withColumn(
                "_numero_version_scd2",
                F.row_number().over(
                    scd_window
                )
            )
        )

        # ====================================================
        # 6. VALIDAR FECHA DE REGISTRO
        #
        # La primera versión necesita fecha_registro porque
        # representa desde cuándo el cliente existe realmente
        # para el supermercado.
        # ====================================================

        clientes_sin_fecha_registro = (
            df_scd2
            .filter(
                (
                    F.col(
                        "_numero_version_scd2"
                    ) == 1
                )
                &
                F.col(
                    "fecha_registro"
                ).isNull()
            )
            .count()
        )

        if clientes_sin_fecha_registro > 0:

            raise ValueError(
                "Existen clientes cuya primera versión "
                "no tiene fecha_registro. "
                "No es posible calcular correctamente "
                "fecha_inicio."
            )

        # ====================================================
        # 7. CALCULAR FECHA DE INICIO
        #
        # PRIMERA VERSIÓN:
        #
        # fecha_inicio = fecha_registro
        #
        # VERSIONES POSTERIORES SCD2:
        #
        # fecha_inicio = updated_at
        #
        # fecha_registro:
        # fecha histórica real en la que la persona se
        # convirtió en cliente.
        #
        # updated_at de la primera fila puede representar
        # la migración al nuevo sistema digital.
        #
        # updated_at posterior sí representa cambios
        # capturados por ese sistema.
        # ====================================================

        df_scd2 = (
            df_scd2

            .withColumn(
                "fecha_inicio",

                F.when(
                    F.col(
                        "_numero_version_scd2"
                    ) == 1,

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
        )

        # ====================================================
        # 8. CALCULAR FECHA FIN
        #
        # La fecha_fin de una versión es la fecha_inicio
        # de la siguiente versión SCD2.
        #
        # La versión más reciente queda con:
        #
        # fecha_fin = NULL
        # es_actual = TRUE
        # ====================================================

        df_scd2 = (
            df_scd2

            .withColumn(
                "fecha_fin",

                F.lead(
                    "fecha_inicio"
                ).over(
                    scd_window
                )
            )

            .withColumn(
                "es_actual",

                F.col(
                    "fecha_fin"
                ).isNull()
            )
        )

        # ====================================================
        # 9. APLICAR SCD1
        #
        # Los atributos SCD1 deben contener siempre el valor
        # más reciente conocido del cliente.
        #
        # Aunque existan varias versiones históricas por
        # cambios en ciudad, los valores SCD1 se sobrescriben
        # conceptualmente en todas ellas.
        # ====================================================

        latest_window = (
            Window
            .partitionBy("cliente_id")
            .orderBy(
                F.col(
                    "updated_at"
                ).desc()
            )
        )

        df_latest = (
            df_silver

            .withColumn(
                "_rn",

                F.row_number().over(
                    latest_window
                )
            )

            .filter(
                F.col("_rn") == 1
            )

            .select(
                "cliente_id",

                F.col(
                    "tipo_documento"
                ).alias(
                    "latest_tipo_documento"
                ),

                F.col(
                    "numero_documento"
                ).alias(
                    "latest_numero_documento"
                ),

                F.col(
                    "nombre"
                ).alias(
                    "latest_nombre"
                ),

                F.col(
                    "apellido"
                ).alias(
                    "latest_apellido"
                ),

                F.col(
                    "correo"
                ).alias(
                    "latest_correo"
                ),

                F.col(
                    "telefono"
                ).alias(
                    "latest_telefono"
                ),

                F.col(
                    "fecha_nacimiento"
                ).alias(
                    "latest_fecha_nacimiento"
                ),

                F.col(
                    "activo"
                ).alias(
                    "latest_activo"
                ),
            )
        )

        # ====================================================
        # 10. APLICAR VALORES SCD1 MÁS RECIENTES
        # ====================================================

        df_gold = (
            df_scd2

            .join(
                df_latest,
                on="cliente_id",
                how="left",
            )

            .withColumn(
                "tipo_documento",
                F.col(
                    "latest_tipo_documento"
                )
            )

            .withColumn(
                "numero_documento",
                F.col(
                    "latest_numero_documento"
                )
            )

            .withColumn(
                "nombre",
                F.col(
                    "latest_nombre"
                )
            )

            .withColumn(
                "apellido",
                F.col(
                    "latest_apellido"
                )
            )

            .withColumn(
                "correo",
                F.col(
                    "latest_correo"
                )
            )

            .withColumn(
                "telefono",
                F.col(
                    "latest_telefono"
                )
            )

            .withColumn(
                "fecha_nacimiento",
                F.col(
                    "latest_fecha_nacimiento"
                )
            )

            .withColumn(
                "activo",
                F.col(
                    "latest_activo"
                )
            )
        )

        # ====================================================
        # 11. GENERAR SURROGATE KEY
        #
        # Los clientes REALES empiezan en:
        #
        # cliente_sk = 1
        #
        # Dejamos reservada:
        #
        # cliente_sk = 0
        #
        # para el miembro "Cliente desconocido".
        #
        # En una carga incremental futura utilizaremos:
        #
        # MAX(cliente_sk) + nuevas versiones.
        # ====================================================

        surrogate_window = (
            Window.orderBy(
                "cliente_id",
                "fecha_inicio",
            )
        )

        df_gold = (
            df_gold

            .withColumn(
                "cliente_sk",

                F.row_number()
                .over(
                    surrogate_window
                )
                .cast("long")
            )
        )

        # ====================================================
        # 12. TRAZABILIDAD
        #
        # source_updated_at conserva el timestamp del registro
        # fuente que dio origen a esa versión SCD2.
        #
        # No utilizamos fecha_inicio porque ambos campos
        # representan conceptos diferentes.
        # ====================================================

        df_gold = (
            df_gold

            .withColumn(
                "source_updated_at",
                F.col(
                    "updated_at"
                )
            )
        )

        # ====================================================
        # 13. SELECCIONAR ESQUEMA FINAL
        # ====================================================

        df_gold = (
            df_gold

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

        # ====================================================
        # 14. CREAR MIEMBRO DESCONOCIDO
        #
        # El sistema operacional permite facturas donde:
        #
        # cliente_id = NULL
        #
        # Esto representa una venta anónima o una compra donde
        # el cliente no fue identificado.
        #
        # En el Data Warehouse NO queremos que la FK de
        # fact_ventas quede NULL.
        #
        # Por eso reservamos:
        #
        # cliente_sk = 0
        #
        # para:
        #
        # Cliente desconocido
        #
        # IMPORTANTE:
        #
        # cliente_id permanece NULL porque no existe realmente
        # un cliente operacional correspondiente.
        #
        # La asignación cliente_sk = 0 ocurrirá posteriormente
        # en fact_ventas únicamente cuando cliente_id sea NULL.
        # ====================================================

        unknown_cliente = (
            spark.createDataFrame(
                [
                    (
                        0,                      # cliente_sk
                        None,                   # cliente_id
                        "N/A",                  # tipo_documento
                        "N/A",                  # numero_documento
                        "Cliente desconocido",  # nombre
                        "N/A",                  # apellido
                        "N/A",                  # correo
                        "N/A",                  # telefono
                        "Desconocida",          # ciudad
                        None,                   # fecha_nacimiento
                        True,                   # activo
                        None,                   # fecha_inicio
                        None,                   # fecha_fin
                        True,                   # es_actual
                        None,                   # source_updated_at
                    )
                ],
                schema=df_gold.schema,
            )
        )

        # ====================================================
        # 15. UNIR MIEMBRO DESCONOCIDO CON CLIENTES REALES
        # ====================================================

        df_gold = (
            unknown_cliente
            .unionByName(
                df_gold
            )
        )

        # ====================================================
        # 16. VALIDAR MIEMBRO DESCONOCIDO
        #
        # Debe existir exactamente una fila con:
        #
        # cliente_sk = 0
        #
        # y debe representar el miembro desconocido.
        # ====================================================

        unknown_count = (
            df_gold
            .filter(
                F.col(
                    "cliente_sk"
                ) == 0
            )
            .count()
        )

        if unknown_count != 1:

            raise ValueError(
                "dim_cliente debe contener exactamente "
                "un registro con cliente_sk = 0."
            )

        # ====================================================
        # 17. VALIDAR SK DE CLIENTES REALES
        #
        # Ningún cliente real puede utilizar cliente_sk = 0.
        # ====================================================

        clientes_reales_sk_invalida = (
            df_gold
            .filter(
                F.col(
                    "cliente_id"
                ).isNotNull()
                &
                (
                    F.col(
                        "cliente_sk"
                    ) <= 0
                )
            )
            .count()
        )

        if clientes_reales_sk_invalida > 0:

            raise ValueError(
                "Se encontraron clientes reales utilizando "
                "una surrogate key reservada o inválida."
            )

        # ====================================================
        # 18. VALIDAR UNA VERSIÓN ACTUAL POR CLIENTE REAL
        # ====================================================

        versiones_actuales = (
            df_gold

            .filter(
                F.col(
                    "cliente_id"
                ).isNotNull()
            )

            .groupBy(
                "cliente_id"
            )

            .agg(
                F.sum(
                    F.when(
                        F.col(
                            "es_actual"
                        ) == True,
                        1,
                    ).otherwise(0)
                ).alias(
                    "versiones_actuales"
                )
            )

            .filter(
                F.col(
                    "versiones_actuales"
                ) != 1
            )

            .count()
        )

        if versiones_actuales > 0:

            raise ValueError(
                "Existen clientes que no tienen exactamente "
                "una versión actual en dim_cliente."
            )

        # ====================================================
        # 19. VALIDAR RANGOS TEMPORALES
        #
        # Para versiones cerradas debe cumplirse:
        #
        # fecha_inicio < fecha_fin
        #
        # El miembro desconocido se excluye porque no
        # representa una entidad histórica real.
        # ====================================================

        rangos_invalidos = (
            df_gold

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

        if rangos_invalidos > 0:

            raise ValueError(
                "Existen rangos temporales inválidos "
                "en dim_cliente."
            )

        # ====================================================
        # 20. GUARDAR GOLD
        #
        # Al ser carga inicial utilizamos overwrite.
        # ====================================================

        print(
            "\nGuardando dim_cliente..."
        )

        (
            df_gold.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true",
            )
            .save(
                GOLD_PATH
            )
        )

        # ====================================================
        # 21. RESULTADOS
        # ====================================================

        print(
            f"Registros Gold creados: "
            f"{df_gold.count()}"
        )

        print(
            "\nMiembro Cliente desconocido:"
        )

        (
            df_gold
            .filter(
                F.col(
                    "cliente_sk"
                ) == 0
            )
            .show(
                truncate=False
            )
        )

        print(
            "\nHistorial dimensional "
            "cliente_id 1 y 2:"
        )

        (
            df_gold

            .filter(
                F.col(
                    "cliente_id"
                ).isin(
                    1,
                    2,
                )
            )

            .orderBy(
                "cliente_id",
                "fecha_inicio",
            )

            .show(
                truncate=False
            )
        )

        print(
            "\ndim_cliente creada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    build_dim_cliente_initial()