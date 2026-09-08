from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import create_spark_session


SILVER_FACTURAS_PATH = "/opt/spark-data/silver/facturas"
SILVER_DETALLES_PATH = "/opt/spark-data/silver/detalles_factura"

DIM_CLIENTE_PATH = "/opt/spark-data/gold/dim_cliente"
DIM_PRODUCTO_PATH = "/opt/spark-data/gold/dim_producto"
DIM_SUCURSAL_PATH = "/opt/spark-data/gold/dim_sucursal"
DIM_VENDEDOR_PATH = "/opt/spark-data/gold/dim_vendedor"
DIM_FECHA_PATH = "/opt/spark-data/gold/dim_fecha"
DIM_HORA_PATH = "/opt/spark-data/gold/dim_hora"
DIM_CONTEXTO_PATH = "/opt/spark-data/gold/dim_contexto_venta"

FACT_VENTAS_PATH = "/opt/spark-data/gold/fact_ventas"

UNKNOWN_CLIENTE_SK = 0


def update_fact_ventas():

    spark = create_spark_session(
        "gold_fact_ventas_incremental"
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n====================================")
        print("GOLD INCREMENTAL - FACT_VENTAS")
        print("====================================")

        # ====================================================
        # 1. LEER SILVER
        # ====================================================

        print("\nLeyendo facturas Silver...")

        facturas = (
            spark.read
            .format("delta")
            .load(SILVER_FACTURAS_PATH)
        )

        print("Leyendo detalles_factura Silver...")

        detalles = (
            spark.read
            .format("delta")
            .load(SILVER_DETALLES_PATH)
        )

        # ====================================================
        # 2. LEER FACT_VENTAS ACTUAL
        # ====================================================

        print("Leyendo fact_ventas Gold...")

        fact_actual = (
            spark.read
            .format("delta")
            .load(FACT_VENTAS_PATH)
        )

        # ====================================================
        # 3. IDENTIFICAR DETALLES NUEVOS
        #
        # detalle_id es nuestra clave técnica de origen.
        #
        # left_anti:
        # Silver - detalle_id existentes en Gold
        # ====================================================

        nuevos_detalles = (
            detalles.alias("d")

            .join(
                fact_actual
                .select("detalle_id")
                .alias("f"),

                F.col("d.detalle_id")
                ==
                F.col("f.detalle_id"),

                "left_anti",
            )
        )

        total_nuevos_detalles = (
            nuevos_detalles.count()
        )

        print(
            f"\nNuevas líneas de detalle encontradas: "
            f"{total_nuevos_detalles}"
        )

        # ====================================================
        # SI NO HAY NADA NUEVO, TERMINAR CORRECTAMENTE
        # ====================================================

        if total_nuevos_detalles == 0:

            print(
                "\nNo existen nuevas ventas para procesar."
            )

            print(
                "fact_ventas ya está actualizada."
            )

            return

        # ====================================================
        # 4. UNIR DETALLES NUEVOS CON CABECERA DE FACTURA
        # ====================================================

        ventas = (
            nuevos_detalles.alias("d")

            .join(
                facturas.alias("f"),

                F.col("d.factura_id")
                ==
                F.col("f.factura_id"),

                "inner",
            )

            .select(

                F.col(
                    "d.detalle_id"
                ).alias(
                    "detalle_id"
                ),

                F.col(
                    "d.factura_id"
                ).alias(
                    "factura_id"
                ),

                F.col(
                    "f.numero_factura"
                ).alias(
                    "numero_factura"
                ),

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

                F.col(
                    "f.fecha_hora"
                ).alias(
                    "fecha_hora"
                ),

                F.col(
                    "f.metodo_de_pago"
                ).alias(
                    "metodo_de_pago"
                ),

                F.col(
                    "f.estado"
                ).alias(
                    "estado"
                ),

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
        # 5. VALIDAR QUE TODOS LOS DETALLES TENGAN FACTURA
        # ====================================================

        total_ventas_base = (
            ventas.count()
        )

        if (
            total_ventas_base
            !=
            total_nuevos_detalles
        ):

            raise ValueError(
                "Existen detalles nuevos sin factura "
                "correspondiente en Silver."
            )

        # ====================================================
        # 6. LEER DIMENSIONES
        # ====================================================

        print(
            "\nLeyendo dimensiones Gold..."
        )

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
        # 7. VALIDAR CLIENTE DESCONOCIDO
        # ====================================================

        unknown_count = (
            dim_cliente

            .filter(
                F.col(
                    "cliente_sk"
                )
                ==
                UNKNOWN_CLIENTE_SK
            )

            .count()
        )

        if unknown_count != 1:

            raise ValueError(
                "dim_cliente debe contener exactamente "
                "un registro cliente_sk = 0."
            )

        # ====================================================
        # 8. LOOKUP HISTÓRICO - CLIENTE
        #
        # Para clientes reales:
        #
        # fecha_hora >= fecha_inicio
        # AND
        # fecha_hora < fecha_fin
        # o fecha_fin IS NULL
        #
        # cliente_sk = 0 se excluye del join.
        # ====================================================

        dc = (
            dim_cliente

            .filter(
                F.col(
                    "cliente_sk"
                )
                !=
                UNKNOWN_CLIENTE_SK
            )

            .select(
                F.col(
                    "cliente_id"
                ).alias(
                    "dc_cliente_id"
                ),

                F.col(
                    "cliente_sk"
                ).alias(
                    "dc_cliente_sk"
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

        ventas = (
            ventas.alias("v")

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
                    (
                        F.col(
                            "v.fecha_hora"
                        )
                        <
                        F.col(
                            "dc.dc_fecha_fin"
                        )
                    )
                    |
                    F.col(
                        "dc.dc_fecha_fin"
                    ).isNull()
                ),

                "left",
            )

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
                        "dc_cliente_sk"
                    )
                )
            )

            .drop(
                "dc_cliente_id",
                "dc_cliente_sk",
                "dc_fecha_inicio",
                "dc_fecha_fin",
            )
        )

        # ====================================================
        # 9. VALIDAR CLIENTES CONOCIDOS
        #
        # cliente_id NOT NULL nunca puede terminar en SK NULL
        # ni en SK 0.
        # ====================================================

        clientes_invalidos = (
            ventas

            .filter(
                F.col(
                    "cliente_id"
                ).isNotNull()
                &
                (
                    F.col(
                        "cliente_sk"
                    ).isNull()
                    |
                    (
                        F.col(
                            "cliente_sk"
                        )
                        ==
                        UNKNOWN_CLIENTE_SK
                    )
                )
            )
        )

        total_clientes_invalidos = (
            clientes_invalidos.count()
        )

        if total_clientes_invalidos > 0:

            print(
                "\nERROR: clientes conocidos "
                "sin correspondencia histórica."
            )

            clientes_invalidos.show(
                50,
                False,
            )

            raise ValueError(
                f"Se encontraron "
                f"{total_clientes_invalidos} "
                f"ventas de clientes conocidos "
                f"sin cliente_sk válida."
            )

        # ====================================================
        # 10. LOOKUP HISTÓRICO - PRODUCTO
        # ====================================================

        dp = (
            dim_producto

            .select(
                F.col(
                    "producto_id"
                ).alias(
                    "dp_producto_id"
                ),

                F.col(
                    "producto_sk"
                ).alias(
                    "dp_producto_sk"
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

        ventas = (
            ventas.alias("v")

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
                    (
                        F.col(
                            "v.fecha_hora"
                        )
                        <
                        F.col(
                            "dp.dp_fecha_fin"
                        )
                    )
                    |
                    F.col(
                        "dp.dp_fecha_fin"
                    ).isNull()
                ),

                "left",
            )

            .withColumn(
                "producto_sk",
                F.col(
                    "dp_producto_sk"
                )
            )

            .drop(
                "dp_producto_id",
                "dp_producto_sk",
                "dp_fecha_inicio",
                "dp_fecha_fin",
            )
        )

        # ====================================================
        # 11. LOOKUP HISTÓRICO - SUCURSAL
        # ====================================================

        ds = (
            dim_sucursal

            .select(
                F.col(
                    "sucursal_id"
                ).alias(
                    "ds_sucursal_id"
                ),

                F.col(
                    "sucursal_sk"
                ).alias(
                    "ds_sucursal_sk"
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

        ventas = (
            ventas.alias("v")

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
                    (
                        F.col(
                            "v.fecha_hora"
                        )
                        <
                        F.col(
                            "ds.ds_fecha_fin"
                        )
                    )
                    |
                    F.col(
                        "ds.ds_fecha_fin"
                    ).isNull()
                ),

                "left",
            )

            .withColumn(
                "sucursal_sk",
                F.col(
                    "ds_sucursal_sk"
                )
            )

            .drop(
                "ds_sucursal_id",
                "ds_sucursal_sk",
                "ds_fecha_inicio",
                "ds_fecha_fin",
            )
        )

        # ====================================================
        # 12. LOOKUP HISTÓRICO - VENDEDOR
        # ====================================================

        dv = (
            dim_vendedor

            .select(
                F.col(
                    "vendedor_id"
                ).alias(
                    "dv_vendedor_id"
                ),

                F.col(
                    "vendedor_sk"
                ).alias(
                    "dv_vendedor_sk"
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

        ventas = (
            ventas.alias("v")

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
                    (
                        F.col(
                            "v.fecha_hora"
                        )
                        <
                        F.col(
                            "dv.dv_fecha_fin"
                        )
                    )
                    |
                    F.col(
                        "dv.dv_fecha_fin"
                    ).isNull()
                ),

                "left",
            )

            .withColumn(
                "vendedor_sk",
                F.col(
                    "dv_vendedor_sk"
                )
            )

            .drop(
                "dv_vendedor_id",
                "dv_vendedor_sk",
                "dv_fecha_inicio",
                "dv_fecha_fin",
            )
        )

        # ====================================================
        # 13. LOOKUP DIM_FECHA
        # ====================================================

        ventas = (
            ventas

            .withColumn(
                "fecha",
                F.to_date(
                    "fecha_hora"
                )
            )

            .join(
                dim_fecha
                .select(
                    "fecha_sk",
                    "fecha",
                ),

                "fecha",

                "left",
            )

            .drop(
                "fecha"
            )
        )

        # ====================================================
        # 14. LOOKUP DIM_HORA
        #
        # Asumimos que dim_hora usa hora y minuto,
        # como en la carga inicial.
        # ====================================================

        ventas = (
            ventas

            .withColumn(
                "hora",
                F.hour(
                    "fecha_hora"
                )
            )

            .withColumn(
                "minuto",
                F.minute(
                    "fecha_hora"
                )
            )

            .join(
                dim_hora
                .select(
                    "hora_sk",
                    "hora",
                    "minuto",
                ),

                [
                    "hora",
                    "minuto",
                ],

                "left",
            )

            .drop(
                "hora",
                "minuto",
            )
        )

        # ====================================================
        # 15. LOOKUP DIM_CONTEXTO_VENTA
        # ====================================================

        ventas = (
            ventas

            .join(
                dim_contexto
                .select(
                    "contexto_venta_sk",
                    "metodo_de_pago",
                    "estado",
                ),

                [
                    "metodo_de_pago",
                    "estado",
                ],

                "left",
            )
        )

        # ====================================================
        # 16. CALCULAR MEDIDAS
        #
        # Grain:
        # una fila = una línea de producto de factura
        # ====================================================

        ventas = (
            ventas

            .withColumn(
                "subtotal_linea",

                (
                    F.col(
                        "cantidad"
                    )
                    *
                    F.col(
                        "precio_unitario"
                    )
                )
            )

            .withColumn(
                "descuento_linea",

                (
                    F.col(
                        "cantidad"
                    )
                    *
                    F.col(
                        "descuento_unitario"
                    )
                )
            )

            .withColumn(
                "impuesto_linea",

                (
                    F.col(
                        "cantidad"
                    )
                    *
                    F.col(
                        "impuesto_unitario"
                    )
                )
            )
        )

        # ====================================================
        # 17. VALIDAR TODAS LAS SURROGATE KEYS
        # ====================================================

        ventas_sin_sk = (
            ventas

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

        total_sin_sk = (
            ventas_sin_sk.count()
        )

        if total_sin_sk > 0:

            print(
                "\nERROR: existen nuevas ventas "
                "sin surrogate key."
            )

            ventas_sin_sk.select(
                "detalle_id",
                "numero_factura",
                "cliente_id",
                "cliente_sk",
                "producto_id",
                "producto_sk",
                "sucursal_id",
                "sucursal_sk",
                "vendedor_id",
                "vendedor_sk",
                "fecha_hora",
                "fecha_sk",
                "hora_sk",
                "metodo_de_pago",
                "estado",
                "contexto_venta_sk",
            ).show(
                100,
                False,
            )

            raise ValueError(
                f"Se encontraron {total_sin_sk} "
                f"líneas nuevas sin correspondencia "
                f"dimensional."
            )

        # ====================================================
        # 18. VALIDAR CLIENTE DESCONOCIDO
        #
        # Solo cliente_id NULL puede usar cliente_sk = 0.
        # ====================================================

        unknown_invalidos = (
            ventas

            .filter(
                (
                    F.col(
                        "cliente_id"
                    ).isNull()
                    &
                    (
                        F.col(
                            "cliente_sk"
                        ).isNull()
                        |
                        (
                            F.col(
                                "cliente_sk"
                            )
                            !=
                            UNKNOWN_CLIENTE_SK
                        )
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

        if unknown_invalidos > 0:

            raise ValueError(
                "Se detectaron asignaciones inválidas "
                "del cliente desconocido."
            )

        # ====================================================
        # 19. VALIDAR GRAIN
        #
        # detalle_id debe aparecer una sola vez.
        # ====================================================

        detalle_duplicado = (
            ventas

            .groupBy(
                "detalle_id"
            )

            .count()

            .filter(
                F.col(
                    "count"
                )
                !=
                1
            )

            .count()
        )

        if detalle_duplicado > 0:

            raise ValueError(
                "El lookup dimensional multiplicó "
                "líneas de detalle. "
                "Se violó el grain de fact_ventas."
            )

        # ====================================================
        # 20. OBTENER MAX VENTA_SK
        # ====================================================

        max_venta_sk = (
            fact_actual

            .agg(
                F.max(
                    "venta_sk"
                ).alias(
                    "max_sk"
                )
            )

            .collect()[0]["max_sk"]
        )

        if max_venta_sk is None:
            max_venta_sk = 0

        # ====================================================
        # 21. GENERAR NUEVAS VENTA_SK
        # ====================================================

        venta_window = (
            Window.orderBy(
                "detalle_id"
            )
        )

        ventas = (
            ventas

            .withColumn(
                "venta_sk",

                (
                    F.lit(
                        max_venta_sk
                    )
                    +
                    F.row_number()
                    .over(
                        venta_window
                    )
                )
                .cast("long")
            )
        )

        # ====================================================
        # 22. SELECCIONAR ESQUEMA FINAL
        # ====================================================

        nuevas_fact_ventas = (
            ventas

            .select(
                "venta_sk",
                "detalle_id",
                "numero_factura",
                "cliente_sk",
                "producto_sk",
                "sucursal_sk",
                "vendedor_sk",
                "fecha_sk",
                "hora_sk",
                "contexto_venta_sk",
                "cantidad",
                "precio_unitario",
                "subtotal_linea",
                "descuento_linea",
                "impuesto_linea",
                "total_linea",
            )
        )

        # ====================================================
        # 23. VALIDACIÓN FINAL ANTES DE INSERTAR
        # ====================================================

        total_a_insertar = (
            nuevas_fact_ventas.count()
        )

        if (
            total_a_insertar
            !=
            total_nuevos_detalles
        ):

            raise ValueError(
                "La cantidad de filas a insertar en "
                "fact_ventas no coincide con los "
                "nuevos detalles de Silver."
            )

        # ====================================================
        # 24. APPEND
        # ====================================================

        print(
            f"\nInsertando "
            f"{total_a_insertar} "
            f"nuevas filas en fact_ventas..."
        )

        (
            nuevas_fact_ventas.write
            .format("delta")
            .mode("append")
            .save(
                FACT_VENTAS_PATH
            )
        )

        # ====================================================
        # 25. VALIDACIONES POST-INSERT
        # ====================================================

        fact_final = (
            spark.read
            .format("delta")
            .load(
                FACT_VENTAS_PATH
            )
        )

        # detalle_id debe seguir siendo único

        duplicate_detalle_final = (
            fact_final

            .groupBy(
                "detalle_id"
            )

            .count()

            .filter(
                F.col(
                    "count"
                )
                >
                1
            )

            .count()
        )

        if duplicate_detalle_final > 0:

            raise ValueError(
                "fact_ventas contiene detalle_id duplicados "
                "después del incremental."
            )

        # venta_sk también debe ser única

        duplicate_venta_sk = (
            fact_final

            .groupBy(
                "venta_sk"
            )

            .count()

            .filter(
                F.col(
                    "count"
                )
                >
                1
            )

            .count()
        )

        if duplicate_venta_sk > 0:

            raise ValueError(
                "fact_ventas contiene venta_sk duplicadas."
            )

        # ====================================================
        # 26. RESULTADO
        # ====================================================

        ventas_anonimas = (
            nuevas_fact_ventas

            .filter(
                F.col(
                    "cliente_sk"
                )
                ==
                UNKNOWN_CLIENTE_SK
            )

            .count()
        )

        print("\n====================================")
        print("RESULTADO INCREMENTAL FACT_VENTAS")
        print("====================================")

        print(
            f"Nuevos detalles detectados: "
            f"{total_nuevos_detalles}"
        )

        print(
            f"Nuevas filas insertadas: "
            f"{total_a_insertar}"
        )

        print(
            f"Nuevas líneas anónimas "
            f"(cliente_sk = 0): "
            f"{ventas_anonimas}"
        )

        print(
            f"Total actual fact_ventas: "
            f"{fact_final.count()}"
        )

        print(
            "\nÚltimas ventas insertadas:"
        )

        (
            fact_final

            .orderBy(
                F.col(
                    "venta_sk"
                ).desc()
            )

            .show(
                30,
                False,
            )
        )

        print(
            "\nfact_ventas incremental "
            "procesada correctamente."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    update_fact_ventas()