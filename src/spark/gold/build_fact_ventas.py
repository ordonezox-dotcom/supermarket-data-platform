from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import create_spark_session


# ============================================================
# SILVER
# ============================================================

SILVER_FACTURAS_PATH = "/opt/spark-data/silver/facturas"
SILVER_DETALLE_PATH = "/opt/spark-data/silver/detalles_factura"


# ============================================================
# GOLD DIMENSIONS
# ============================================================

DIM_CLIENTE_PATH = "/opt/spark-data/gold/dim_cliente"
DIM_PRODUCTO_PATH = "/opt/spark-data/gold/dim_producto"
DIM_SUCURSAL_PATH = "/opt/spark-data/gold/dim_sucursal"
DIM_VENDEDOR_PATH = "/opt/spark-data/gold/dim_vendedor"
DIM_FECHA_PATH = "/opt/spark-data/gold/dim_fecha"
DIM_HORA_PATH = "/opt/spark-data/gold/dim_hora"
DIM_CONTEXTO_PATH = "/opt/spark-data/gold/dim_contexto_venta"


# ============================================================
# GOLD FACT
# ============================================================

GOLD_PATH = "/opt/spark-data/gold/fact_ventas"


# ============================================================
# SURROGATE KEY RESERVADA
# ============================================================

UNKNOWN_CLIENTE_SK = 0


def build_fact_ventas():

    spark = create_spark_session(
        "gold_fact_ventas"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD - FACT_VENTAS")
        print("====================================")

        # ====================================================
        # 1. LEER SILVER
        # ====================================================

        print("\nLeyendo facturas Silver...")

        df_facturas = (
            spark.read
            .format("delta")
            .load(SILVER_FACTURAS_PATH)
        )

        print("\nLeyendo detalle_factura Silver...")

        df_detalle = (
            spark.read
            .format("delta")
            .load(SILVER_DETALLE_PATH)
        )

        print(
            f"Facturas: {df_facturas.count()}"
        )

        print(
            f"Líneas detalle: {df_detalle.count()}"
        )

        # ====================================================
        # 2. LEER DIMENSIONES GOLD
        # ====================================================

        print("\nLeyendo dimensiones Gold...")

        dim_cliente = (
            spark.read
            .format("delta")
            .load(DIM_CLIENTE_PATH)
        )

        dim_producto = (
            spark.read
            .format("delta")
            .load(DIM_PRODUCTO_PATH)
        )

        dim_sucursal = (
            spark.read
            .format("delta")
            .load(DIM_SUCURSAL_PATH)
        )

        dim_vendedor = (
            spark.read
            .format("delta")
            .load(DIM_VENDEDOR_PATH)
        )

        dim_fecha = (
            spark.read
            .format("delta")
            .load(DIM_FECHA_PATH)
        )

        dim_hora = (
            spark.read
            .format("delta")
            .load(DIM_HORA_PATH)
        )

        dim_contexto = (
            spark.read
            .format("delta")
            .load(DIM_CONTEXTO_PATH)
        )

        # ====================================================
        # 3. VALIDAR MIEMBRO DESCONOCIDO EN DIM_CLIENTE
        #
        # fact_ventas necesita utilizar cliente_sk = 0
        # para ventas anónimas.
        # ====================================================

        unknown_cliente_count = (
            dim_cliente
            .filter(
                F.col("cliente_sk") == UNKNOWN_CLIENTE_SK
            )
            .count()
        )

        if unknown_cliente_count != 1:

            raise ValueError(
                "dim_cliente debe contener exactamente un "
                "registro con cliente_sk = 0 para representar "
                "Cliente desconocido."
            )

        print(
            "Cliente desconocido disponible: "
            f"cliente_sk = {UNKNOWN_CLIENTE_SK}"
        )

        # ====================================================
        # 4. UNIR CABECERA Y DETALLE
        #
        # Grain:
        #
        # una fila = una línea de producto de una factura
        # ====================================================

        print(
            "\nConstruyendo grain de fact_ventas..."
        )

        df_ventas = (
            df_detalle.alias("d")

            .join(
                df_facturas.alias("f"),

                F.col("d.factura_id")
                ==
                F.col("f.factura_id"),

                "inner",
            )

            .select(

                # Llaves técnicas de origen

                F.col(
                    "d.detalle_id"
                ).alias(
                    "detalle_id"
                ),

                F.col(
                    "f.factura_id"
                ).alias(
                    "factura_id"
                ),

                # Degenerate dimension

                F.col(
                    "f.numero_factura"
                ).alias(
                    "numero_factura"
                ),

                # Natural keys

                F.col(
                    "f.cliente_id"
                ).alias(
                    "cliente_id"
                ),

                F.col(
                    "d.producto_id"
                ).alias(
                    "producto_id"
                ),

                F.col(
                    "f.sucursal_id"
                ).alias(
                    "sucursal_id"
                ),

                F.col(
                    "f.vendedor_id"
                ).alias(
                    "vendedor_id"
                ),

                # Fecha de la venta

                F.col(
                    "f.fecha_hora"
                ).alias(
                    "fecha_hora"
                ),

                # Contexto

                F.upper(
                    F.trim(
                        F.col(
                            "f.metodo_de_pago"
                        )
                    )
                ).alias(
                    "metodo_de_pago"
                ),

                F.upper(
                    F.trim(
                        F.col(
                            "f.estado"
                        )
                    )
                ).alias(
                    "estado"
                ),

                # Medidas

                F.col(
                    "d.cantidad"
                ).alias(
                    "cantidad"
                ),

                F.col(
                    "d.precio_unitario"
                ).alias(
                    "precio_unitario"
                ),

                F.col(
                    "d.descuento_unitario"
                ).alias(
                    "descuento_unitario"
                ),

                F.col(
                    "d.impuesto_unitario"
                ).alias(
                    "impuesto_unitario"
                ),

                F.col(
                    "d.total_linea"
                ).alias(
                    "total_linea"
                ),
            )
        )

        # ====================================================
        # 5. CALCULAR MEDIDAS DE LA LÍNEA
        #
        # No usamos los totales de la cabecera porque el grain
        # de fact_ventas está a nivel de línea de producto.
        # ====================================================

        df_ventas = (
            df_ventas

            .withColumn(
                "subtotal_linea",

                F.col("cantidad")
                *
                F.col("precio_unitario")
            )

            .withColumn(
                "descuento_linea",

                F.col("cantidad")
                *
                F.col("descuento_unitario")
            )

            .withColumn(
                "impuesto_linea",

                F.col("cantidad")
                *
                F.col("impuesto_unitario")
            )
        )

        # ====================================================
        # 6. PREPARAR FECHA Y HORA
        # ====================================================

        df_ventas = (
            df_ventas

            .withColumn(
                "fecha_venta",
                F.to_date(
                    "fecha_hora"
                )
            )

            .withColumn(
                "hora_venta",
                F.hour(
                    "fecha_hora"
                )
            )

            .withColumn(
                "minuto_venta",
                F.minute(
                    "fecha_hora"
                )
            )
        )

        # ====================================================
        # 7. LOOKUP DIM_CLIENTE - SCD2
        #
        # Para clientes reales:
        #
        # cliente_id debe coincidir
        #
        # fecha_hora >= fecha_inicio
        #
        # fecha_hora < fecha_fin
        #
        # o fecha_fin IS NULL
        #
        # Para ventas anónimas:
        #
        # cliente_id = NULL
        #
        # se asignará posteriormente cliente_sk = 0.
        # ====================================================

        print(
            "\nResolviendo cliente_sk histórico..."
        )

        dc = (
            dim_cliente

            # Excluimos el miembro desconocido del lookup SCD2.
            # Este registro se asigna explícitamente después.
            .filter(
                F.col(
                    "cliente_sk"
                ) != UNKNOWN_CLIENTE_SK
            )

            .select(
                F.col(
                    "cliente_sk"
                ).alias(
                    "dc_cliente_sk"
                ),

                F.col(
                    "cliente_id"
                ).alias(
                    "dc_cliente_id"
                ),

                F.col(
                    "fecha_inicio"
                ).alias(
                    "dc_fecha_inicio"
                ),

                F.col(
                    "fecha_fin"
                ).alias(
                    "dc_fecha_fin"
                ),
            )
        )

        df_ventas = (
            df_ventas.alias("v")

            .join(
                dc.alias("dc"),

                (
                    F.col(
                        "v.cliente_id"
                    )
                    ==
                    F.col(
                        "dc.dc_cliente_id"
                    )
                )
                &
                (
                    F.col(
                        "v.fecha_hora"
                    )
                    >=
                    F.col(
                        "dc.dc_fecha_inicio"
                    )
                )
                &
                (
                    F.col(
                        "dc.dc_fecha_fin"
                    ).isNull()

                    |

                    (
                        F.col(
                            "v.fecha_hora"
                        )
                        <
                        F.col(
                            "dc.dc_fecha_fin"
                        )
                    )
                ),

                "left",
            )

            .select(
                "v.*",

                F.col(
                    "dc.dc_cliente_sk"
                ).alias(
                    "cliente_sk"
                ),
            )
        )

        # ====================================================
        # 8. ASIGNAR CLIENTE DESCONOCIDO
        #
        # MUY IMPORTANTE:
        #
        # Solo asignamos cliente_sk = 0 cuando el dato fuente
        # realmente viene sin cliente:
        #
        # cliente_id IS NULL
        #
        # NO usamos coalesce(cliente_sk, 0), porque eso podría
        # ocultar un error real del lookup SCD2 para un cliente
        # que sí tiene cliente_id.
        # ====================================================

        df_ventas = (
            df_ventas

            .withColumn(
                "cliente_sk",

                F.when(
                    F.col(
                        "cliente_id"
                    ).isNull(),

                    F.lit(
                        UNKNOWN_CLIENTE_SK
                    ).cast("long")
                )

                .otherwise(
                    F.col(
                        "cliente_sk"
                    )
                )
            )
        )

        ventas_anonimas = (
            df_ventas
            .filter(
                F.col(
                    "cliente_id"
                ).isNull()
            )
            .count()
        )

        print(
            f"Líneas de ventas anónimas asignadas a "
            f"cliente_sk = 0: {ventas_anonimas}"
        )

        # ====================================================
        # 9. LOOKUP DIM_PRODUCTO - SCD2
        # ====================================================

        print(
            "Resolviendo producto_sk histórico..."
        )

        dp = (
            dim_producto
            .select(
                F.col(
                    "producto_sk"
                ).alias(
                    "dp_producto_sk"
                ),

                F.col(
                    "producto_id"
                ).alias(
                    "dp_producto_id"
                ),

                F.col(
                    "fecha_inicio"
                ).alias(
                    "dp_fecha_inicio"
                ),

                F.col(
                    "fecha_fin"
                ).alias(
                    "dp_fecha_fin"
                ),
            )
        )

        df_ventas = (
            df_ventas.alias("v")

            .join(
                dp.alias("dp"),

                (
                    F.col(
                        "v.producto_id"
                    )
                    ==
                    F.col(
                        "dp.dp_producto_id"
                    )
                )
                &
                (
                    F.col(
                        "v.fecha_hora"
                    )
                    >=
                    F.col(
                        "dp.dp_fecha_inicio"
                    )
                )
                &
                (
                    F.col(
                        "dp.dp_fecha_fin"
                    ).isNull()

                    |

                    (
                        F.col(
                            "v.fecha_hora"
                        )
                        <
                        F.col(
                            "dp.dp_fecha_fin"
                        )
                    )
                ),

                "left",
            )

            .select(
                "v.*",

                F.col(
                    "dp.dp_producto_sk"
                ).alias(
                    "producto_sk"
                ),
            )
        )

        # ====================================================
        # 10. LOOKUP DIM_SUCURSAL - SCD2
        # ====================================================

        print(
            "Resolviendo sucursal_sk histórico..."
        )

        ds = (
            dim_sucursal
            .select(
                F.col(
                    "sucursal_sk"
                ).alias(
                    "ds_sucursal_sk"
                ),

                F.col(
                    "sucursal_id"
                ).alias(
                    "ds_sucursal_id"
                ),

                F.col(
                    "fecha_inicio"
                ).alias(
                    "ds_fecha_inicio"
                ),

                F.col(
                    "fecha_fin"
                ).alias(
                    "ds_fecha_fin"
                ),
            )
        )

        df_ventas = (
            df_ventas.alias("v")

            .join(
                ds.alias("ds"),

                (
                    F.col(
                        "v.sucursal_id"
                    )
                    ==
                    F.col(
                        "ds.ds_sucursal_id"
                    )
                )
                &
                (
                    F.col(
                        "v.fecha_hora"
                    )
                    >=
                    F.col(
                        "ds.ds_fecha_inicio"
                    )
                )
                &
                (
                    F.col(
                        "ds.ds_fecha_fin"
                    ).isNull()

                    |

                    (
                        F.col(
                            "v.fecha_hora"
                        )
                        <
                        F.col(
                            "ds.ds_fecha_fin"
                        )
                    )
                ),

                "left",
            )

            .select(
                "v.*",

                F.col(
                    "ds.ds_sucursal_sk"
                ).alias(
                    "sucursal_sk"
                ),
            )
        )

        # ====================================================
        # 11. LOOKUP DIM_VENDEDOR - SCD2
        # ====================================================

        print(
            "Resolviendo vendedor_sk histórico..."
        )

        dv = (
            dim_vendedor
            .select(
                F.col(
                    "vendedor_sk"
                ).alias(
                    "dv_vendedor_sk"
                ),

                F.col(
                    "vendedor_id"
                ).alias(
                    "dv_vendedor_id"
                ),

                F.col(
                    "fecha_inicio"
                ).alias(
                    "dv_fecha_inicio"
                ),

                F.col(
                    "fecha_fin"
                ).alias(
                    "dv_fecha_fin"
                ),
            )
        )

        df_ventas = (
            df_ventas.alias("v")

            .join(
                dv.alias("dv"),

                (
                    F.col(
                        "v.vendedor_id"
                    )
                    ==
                    F.col(
                        "dv.dv_vendedor_id"
                    )
                )
                &
                (
                    F.col(
                        "v.fecha_hora"
                    )
                    >=
                    F.col(
                        "dv.dv_fecha_inicio"
                    )
                )
                &
                (
                    F.col(
                        "dv.dv_fecha_fin"
                    ).isNull()

                    |

                    (
                        F.col(
                            "v.fecha_hora"
                        )
                        <
                        F.col(
                            "dv.dv_fecha_fin"
                        )
                    )
                ),

                "left",
            )

            .select(
                "v.*",

                F.col(
                    "dv.dv_vendedor_sk"
                ).alias(
                    "vendedor_sk"
                ),
            )
        )

        # ====================================================
        # 12. RESOLVER FECHA_SK
        #
        # dim_fecha:
        #
        # fecha_sk = YYYYMMDD
        #
        # Hacemos JOIN para garantizar que la fecha realmente
        # exista en la dimensión.
        # ====================================================

        print(
            "Resolviendo fecha_sk..."
        )

        df_ventas = (
            df_ventas.alias("v")

            .join(
                dim_fecha
                .select(
                    "fecha_sk",
                    "fecha",
                )
                .alias("df"),

                F.col(
                    "v.fecha_venta"
                )
                ==
                F.col(
                    "df.fecha"
                ),

                "left",
            )

            .select(
                "v.*",

                F.col(
                    "df.fecha_sk"
                ).alias(
                    "fecha_sk"
                ),
            )
        )

        # ====================================================
        # 13. RESOLVER HORA_SK
        #
        # dim_hora tiene una fila por minuto.
        #
        # hora_sk = hora * 100 + minuto
        # ====================================================

        print(
            "Resolviendo hora_sk..."
        )

        df_ventas = (
            df_ventas.alias("v")

            .join(
                dim_hora
                .select(
                    "hora_sk",
                    "hora",
                    "minuto",
                )
                .alias("dh"),

                (
                    F.col(
                        "v.hora_venta"
                    )
                    ==
                    F.col(
                        "dh.hora"
                    )
                )
                &
                (
                    F.col(
                        "v.minuto_venta"
                    )
                    ==
                    F.col(
                        "dh.minuto"
                    )
                ),

                "left",
            )

            .select(
                "v.*",

                F.col(
                    "dh.hora_sk"
                ).alias(
                    "hora_sk"
                ),
            )
        )

        # ====================================================
        # 14. RESOLVER CONTEXTO_VENTA_SK
        # ====================================================

        print(
            "Resolviendo contexto_venta_sk..."
        )

        df_ventas = (
            df_ventas.alias("v")

            .join(
                dim_contexto.alias("dcv"),

                (
                    F.col(
                        "v.metodo_de_pago"
                    )
                    ==
                    F.col(
                        "dcv.metodo_de_pago"
                    )
                )
                &
                (
                    F.col(
                        "v.estado"
                    )
                    ==
                    F.col(
                        "dcv.estado"
                    )
                ),

                "left",
            )

            .select(
                "v.*",

                F.col(
                    "dcv.contexto_venta_sk"
                ).alias(
                    "contexto_venta_sk"
                ),
            )
        )

        # ====================================================
        # 15. VALIDAR CLIENTES REALES SIN CORRESPONDENCIA
        #
        # Esta validación es importante porque cliente_sk = 0
        # solo puede utilizarse cuando cliente_id es NULL.
        #
        # Si cliente_id tiene valor pero cliente_sk quedó NULL,
        # existe un problema real de dimensión o temporalidad.
        # ====================================================

        clientes_reales_sin_sk = (
            df_ventas
            .filter(
                F.col(
                    "cliente_id"
                ).isNotNull()
                &
                F.col(
                    "cliente_sk"
                ).isNull()
            )
        )

        total_clientes_reales_sin_sk = (
            clientes_reales_sin_sk.count()
        )

        if total_clientes_reales_sin_sk > 0:

            print(
                "\nERROR: existen ventas de clientes reales "
                "sin cliente_sk."
            )

            (
                clientes_reales_sin_sk

                .select(
                    "factura_id",
                    "detalle_id",
                    "numero_factura",
                    "cliente_id",
                    "fecha_hora",
                    "cliente_sk",
                )

                .show(
                    50,
                    truncate=False,
                )
            )

            raise ValueError(
                f"Se encontraron "
                f"{total_clientes_reales_sin_sk} líneas de "
                f"clientes identificados sin cliente_sk."
            )

        # ====================================================
        # 16. VALIDAR QUE TODAS LAS DIMENSIONES FUERON
        #     RESUELTAS
        #
        # Las ventas anónimas ya tienen:
        #
        # cliente_sk = 0
        #
        # Por tanto, cualquier NULL restante representa un
        # error dimensional real.
        # ====================================================

        df_errores_dimensiones = (
            df_ventas
            .filter(

                F.col(
                    "cliente_sk"
                ).isNull()

                |

                F.col(
                    "producto_sk"
                ).isNull()

                |

                F.col(
                    "sucursal_sk"
                ).isNull()

                |

                F.col(
                    "vendedor_sk"
                ).isNull()

                |

                F.col(
                    "fecha_sk"
                ).isNull()

                |

                F.col(
                    "hora_sk"
                ).isNull()

                |

                F.col(
                    "contexto_venta_sk"
                ).isNull()
            )
        )

        errores_dimensiones = (
            df_errores_dimensiones.count()
        )

        if errores_dimensiones > 0:

            print(
                "\nERROR: existen ventas sin "
                "surrogate key."
            )

            (
                df_errores_dimensiones

                .select(
                    "factura_id",
                    "detalle_id",
                    "numero_factura",
                    "cliente_id",
                    "producto_id",
                    "sucursal_id",
                    "vendedor_id",
                    "fecha_hora",
                    "cliente_sk",
                    "producto_sk",
                    "sucursal_sk",
                    "vendedor_sk",
                    "fecha_sk",
                    "hora_sk",
                    "contexto_venta_sk",
                )

                .show(
                    50,
                    truncate=False,
                )
            )

            raise ValueError(
                f"Se encontraron "
                f"{errores_dimensiones} líneas "
                f"sin correspondencia dimensional."
            )

        # ====================================================
        # 17. VALIDAR ASIGNACIÓN DE CLIENTE DESCONOCIDO
        #
        # Debe cumplirse:
        #
        # cliente_id IS NULL
        #       -> cliente_sk = 0
        #
        # cliente_id IS NOT NULL
        #       -> cliente_sk != 0
        # ====================================================

        asignaciones_unknown_invalidas = (
            df_ventas

            .filter(
                (
                    F.col(
                        "cliente_id"
                    ).isNull()
                    &
                    (
                        F.col(
                            "cliente_sk"
                        )
                        !=
                        UNKNOWN_CLIENTE_SK
                    )
                )
                |
                (
                    F.col(
                        "cliente_id"
                    ).isNotNull()
                    &
                    (
                        F.col(
                            "cliente_sk"
                        )
                        ==
                        UNKNOWN_CLIENTE_SK
                    )
                )
            )

            .count()
        )

        if asignaciones_unknown_invalidas > 0:

            raise ValueError(
                "Se detectaron asignaciones incorrectas de "
                "cliente_sk = 0."
            )

        # ====================================================
        # 18. GENERAR VENTA_SK
        #
        # Carga inicial:
        #
        # usamos row_number ordenado por detalle_id.
        #
        # detalle_id es la llave técnica de la línea en el
        # sistema fuente.
        #
        # En incremental posteriormente utilizaremos:
        #
        # MAX(venta_sk) + ROW_NUMBER()
        # ====================================================

        print(
            "\nGenerando venta_sk..."
        )

        venta_window = (
            Window.orderBy(
                "detalle_id"
            )
        )

        df_fact_ventas = (
            df_ventas

            .withColumn(
                "venta_sk",

                F.row_number()
                .over(
                    venta_window
                )
                .cast("long")
            )
        )

        # ====================================================
        # 19. ESQUEMA FINAL DE FACT_VENTAS
        #
        # detalle_id se conserva para lineage, incrementalidad
        # e idempotencia.
        # ====================================================

        df_fact_ventas = (
            df_fact_ventas

            .select(

                "venta_sk",

                # Llave técnica de origen

                "detalle_id",

                # Degenerate dimension

                "numero_factura",

                # Foreign keys

                "cliente_sk",
                "producto_sk",
                "sucursal_sk",
                "vendedor_sk",
                "fecha_sk",
                "hora_sk",
                "contexto_venta_sk",

                # Measures

                "cantidad",
                "precio_unitario",
                "subtotal_linea",
                "descuento_linea",
                "impuesto_linea",
                "total_linea",
            )
        )

        # ====================================================
        # 20. VALIDAR GRAIN
        #
        # detalle_id debe aparecer exactamente una vez.
        # ====================================================

        total_filas = (
            df_fact_ventas.count()
        )

        total_detalles_unicos = (
            df_fact_ventas

            .select(
                "detalle_id"
            )

            .distinct()

            .count()
        )

        if (
            total_filas
            !=
            total_detalles_unicos
        ):

            raise ValueError(
                "Se detectaron duplicados en fact_ventas. "
                "El grain una línea por detalle_id "
                "no se está respetando."
            )

        # ====================================================
        # 21. VALIDAR CANTIDAD DE LÍNEAS
        #
        # Como el grain parte de detalles_factura y el join
        # inicial es inner por factura_id, esperamos una línea
        # final por cada detalle válido.
        # ====================================================

        total_detalle_source = (
            df_detalle.count()
        )

        if (
            total_filas
            !=
            total_detalle_source
        ):

            raise ValueError(
                "La cantidad de filas de fact_ventas no "
                "coincide con la cantidad de líneas de "
                "detalles_factura."
            )

        # ====================================================
        # 22. GUARDAR GOLD
        #
        # Esta sigue siendo una carga inicial.
        # ====================================================

        print(
            "\nGuardando fact_ventas..."
        )

        (
            df_fact_ventas.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true"
            )
            .save(
                GOLD_PATH
            )
        )

        # ====================================================
        # 23. RESULTADOS
        # ====================================================

        print("\n====================================")
        print("RESULTADO FACT_VENTAS")
        print("====================================")

        print(
            f"Líneas cargadas: "
            f"{total_filas}"
        )

        print(
            f"Líneas asignadas a Cliente desconocido: "
            f"{ventas_anonimas}"
        )

        print(
            "\nEjemplo de fact_ventas:"
        )

        (
            df_fact_ventas
            .orderBy(
                "venta_sk"
            )
            .show(
                30,
                truncate=False,
            )
        )

        # ====================================================
        # 24. MOSTRAR ALGUNAS VENTAS ANÓNIMAS
        # ====================================================

        print(
            "\nEjemplo de líneas asociadas "
            "a Cliente desconocido:"
        )

        (
            df_fact_ventas

            .filter(
                F.col(
                    "cliente_sk"
                ) == UNKNOWN_CLIENTE_SK
            )

            .orderBy(
                "venta_sk"
            )

            .show(
                30,
                truncate=False,
            )
        )

        print(
            "\nfact_ventas creada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    build_fact_ventas()