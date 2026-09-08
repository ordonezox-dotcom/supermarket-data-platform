from pyspark.sql import functions as F

from common import create_spark_session


DIM_CLIENTE_PATH = "/opt/spark-data/gold/dim_cliente"
SILVER_FACTURAS_PATH = "/opt/spark-data/silver/facturas"


spark = create_spark_session(
    "test_dim_cliente"
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 1. CARGAR DIM_CLIENTE
# ============================================================

dim_cliente = (
    spark.read
    .format("delta")
    .load(DIM_CLIENTE_PATH)
)


# ============================================================
# 2. MOSTRAR CLIENTE DESCONOCIDO
# ============================================================

print("\n========================================")
print("CLIENTE DESCONOCIDO EN DIM_CLIENTE")
print("========================================")

cliente_desconocido_dim = (
    dim_cliente
    .filter(
        F.col("cliente_sk") == 0
    )
)

total_unknown_dim = (
    cliente_desconocido_dim.count()
)


print(
    f"Registros con cliente_sk = 0: "
    f"{total_unknown_dim}"
)


if total_unknown_dim == 1:

    print(
        "\nRegistro reservado para "
        "Cliente desconocido:"
    )

    cliente_desconocido_dim.select(
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
    ).show(
        truncate=False
    )

elif total_unknown_dim == 0:

    print(
        "\nERROR: dim_cliente todavía no contiene "
        "el registro Cliente desconocido."
    )

else:

    print(
        "\nERROR: existe más de un registro "
        "con cliente_sk = 0."
    )


# ============================================================
# 3. MOSTRAR ALGUNOS CLIENTES REALES
# ============================================================

print("\n========================================")
print("EJEMPLOS DE CLIENTES REALES")
print("========================================")

(
    dim_cliente

    .filter(
        F.col(
            "cliente_id"
        ).isNotNull()
    )

    .select(
        "cliente_sk",
        "cliente_id",
        "nombre",
        "apellido",
        "ciudad",
        "fecha_inicio",
        "fecha_fin",
        "es_actual",
        "source_updated_at",
    )

    .orderBy(
        "cliente_id",
        "fecha_inicio",
    )

    .show(
        30,
        truncate=False,
    )
)


# ============================================================
# 4. CARGAR FACTURAS SILVER
# ============================================================

facturas = (
    spark.read
    .format("delta")
    .load(SILVER_FACTURAS_PATH)
)


# ============================================================
# 5. CONTAR FACTURAS TOTALES
# ============================================================

total_facturas = (
    facturas.count()
)


# ============================================================
# 6. BUSCAR FACTURAS SIN CLIENTE IDENTIFICADO
# ============================================================

facturas_cliente_desconocido = (
    facturas
    .filter(
        F.col(
            "cliente_id"
        ).isNull()
    )
)

total_desconocidas = (
    facturas_cliente_desconocido.count()
)


print("\n========================================")
print("FACTURAS CON CLIENTE DESCONOCIDO")
print("========================================")

print(
    f"Total facturas: "
    f"{total_facturas}"
)

print(
    f"Facturas con cliente_id NULL: "
    f"{total_desconocidas}"
)


if total_facturas > 0:

    porcentaje = (
        total_desconocidas
        / total_facturas
        * 100
    )

    print(
        f"Porcentaje de ventas anónimas: "
        f"{porcentaje:.2f}%"
    )


# ============================================================
# 7. MOSTRAR FACTURAS ANÓNIMAS
# ============================================================

print(
    "\nEjemplos de facturas con "
    "cliente desconocido:"
)

(
    facturas_cliente_desconocido

    .select(
        "factura_id",
        "numero_factura",
        "cliente_id",
        "sucursal_id",
        "vendedor_id",
        "fecha_hora",
        "metodo_de_pago",
        "total",
    )

    .orderBy(
        "factura_id"
    )

    .show(
        30,
        truncate=False,
    )
)


# ============================================================
# 8. VALIDACIÓN FINAL
# ============================================================

print("\n========================================")
print("VALIDACIÓN")
print("========================================")


if (
    total_desconocidas > 0
    and
    total_unknown_dim == 1
):

    print(
        "OK: existen facturas anónimas y "
        "dim_cliente contiene el miembro "
        "Cliente desconocido con cliente_sk = 0."
    )

elif (
    total_desconocidas > 0
    and
    total_unknown_dim == 0
):

    print(
        "ERROR: existen facturas anónimas, "
        "pero dim_cliente no contiene "
        "cliente_sk = 0."
    )

elif total_desconocidas == 0:

    print(
        "No existen facturas anónimas "
        "en Silver."
    )


spark.stop()