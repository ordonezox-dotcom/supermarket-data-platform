import random
from bisect import bisect_right
from datetime import datetime

from faker import Faker
from psycopg2.extensions import connection
from psycopg2.extras import execute_values

from generators.settings import BATCH_SIZE, TOTAL_INVOICES


faker = Faker("es_CO")

random.seed(42)
Faker.seed(42)


PAYMENT_METHODS = [
    "EFECTIVO",
    "TARJETA_DEBITO",
    "TARJETA_CREDITO",
    "PSE",
]


# ============================================================
# PERIODO HISTÓRICO DE VENTAS
#
# Usamos fechas absolutas para que el dataset sea reproducible.
#
# Además, nuestros vendedores fueron generados con fecha de
# contratación máxima 2023-12-31.
# ============================================================

INVOICE_START_DATE = datetime(
    2024, 1, 1, 0, 0, 0
)

INVOICE_END_DATE = datetime(
    2026, 7, 31, 23, 59, 59
)


def get_customers_with_registration(
    conn: connection,
) -> tuple[list[datetime], list[int]]:
    """
    Obtiene clientes ordenados por fecha_registro.

    Devuelve dos listas paralelas:

    registration_dates:
        fechas de registro ordenadas.

    customer_ids:
        cliente correspondiente a cada fecha.

    Esto permite encontrar eficientemente cuáles clientes
    existían cuando ocurrió una factura.
    """

    with conn.cursor() as cursor:

        cursor.execute(
            """
            SELECT
                cliente_id,
                fecha_registro
            FROM clientes
            WHERE fecha_registro IS NOT NULL
            ORDER BY
                fecha_registro,
                cliente_id;
            """
        )

        rows = cursor.fetchall()

    registration_dates = [
        row[1]
        for row in rows
    ]

    customer_ids = [
        row[0]
        for row in rows
    ]

    return (
        registration_dates,
        customer_ids,
    )


def get_sellers_by_store(
    conn: connection,
) -> dict[int, list[tuple[int, object]]]:
    """
    Agrupa vendedores por sucursal conservando también
    fecha_de_contratacion.

    Estructura:

    {
        sucursal_id: [
            (vendedor_id, fecha_de_contratacion),
            ...
        ]
    }
    """

    with conn.cursor() as cursor:

        cursor.execute(
            """
            SELECT
                sucursal_id,
                vendedor_id,
                fecha_de_contratacion
            FROM vendedores
            WHERE fecha_de_contratacion IS NOT NULL
            ORDER BY
                sucursal_id,
                fecha_de_contratacion,
                vendedor_id;
            """
        )

        rows = cursor.fetchall()

    sellers_by_store = {}

    for (
        store_id,
        seller_id,
        hire_date,
    ) in rows:

        sellers_by_store.setdefault(
            store_id,
            [],
        ).append(
            (
                seller_id,
                hire_date,
            )
        )

    return sellers_by_store


def get_valid_customer_id(
    invoice_date: datetime,
    registration_dates: list[datetime],
    customer_ids: list[int],
) -> int | None:
    """
    Selecciona únicamente clientes registrados antes o
    exactamente en la fecha de la factura.

    También conservamos la posibilidad de una venta sin
    cliente identificado.
    """

    # --------------------------------------------------------
    # bisect_right encuentra cuántos clientes tienen:
    #
    # fecha_registro <= fecha_factura
    #
    # Como registration_dates está ordenado, evitamos recorrer
    # todos los clientes para cada factura.
    # --------------------------------------------------------

    valid_count = bisect_right(
        registration_dates,
        invoice_date,
    )

    if valid_count == 0:

        return None

    valid_customer_ids = (
        customer_ids[:valid_count]
    )

    # --------------------------------------------------------
    # Conservamos la posibilidad de cliente NULL.
    #
    # Esto representa una venta donde el comprador no fue
    # identificado.
    # --------------------------------------------------------

    return random.choice(
        valid_customer_ids + [None]
    )


def get_valid_seller_id(
    invoice_date: datetime,
    store_id: int,
    sellers_by_store: dict[int, list[tuple[int, object]]],
) -> int:
    """
    Selecciona un vendedor que:

    1. pertenezca a la sucursal de la factura
    2. ya estuviera contratado cuando ocurrió la venta
    """

    sellers = sellers_by_store.get(
        store_id,
        [],
    )

    valid_sellers = [
        seller_id
        for (
            seller_id,
            hire_date,
        ) in sellers
        if (
            hire_date
            <=
            invoice_date.date()
        )
    ]

    if not valid_sellers:

        raise ValueError(
            f"No existen vendedores válidos para "
            f"sucursal_id={store_id} "
            f"en fecha={invoice_date}."
        )

    return random.choice(
        valid_sellers
    )


def build_invoice(
    invoice_number: int,
    store_ids: list[int],
    sellers_by_store: dict[int, list[tuple[int, object]]],
    registration_dates: list[datetime],
    customer_ids: list[int],
) -> tuple:
    """
    Construye una factura sintética temporalmente coherente.
    """

    # ========================================================
    # 1. GENERAR PRIMERO LA FECHA
    #
    # Esta fecha determinará qué clientes y vendedores
    # realmente podían participar en la transacción.
    # ========================================================

    invoice_date = (
        faker.date_time_between(
            start_date=INVOICE_START_DATE,
            end_date=INVOICE_END_DATE,
        )
    )

    # ========================================================
    # 2. SELECCIONAR SUCURSAL
    #
    # Nuestras 10 sucursales fueron abiertas antes de 2024,
    # por lo tanto todas son válidas dentro del periodo actual
    # de facturación.
    # ========================================================

    store_id = random.choice(
        store_ids
    )

    # ========================================================
    # 3. SELECCIONAR VENDEDOR VÁLIDO
    #
    # Se garantiza:
    #
    # vendedor.sucursal_id = factura.sucursal_id
    #
    # Y:
    #
    # vendedor.fecha_de_contratacion <= factura.fecha_hora
    # ========================================================

    seller_id = get_valid_seller_id(
        invoice_date=invoice_date,
        store_id=store_id,
        sellers_by_store=sellers_by_store,
    )

    # ========================================================
    # 4. SELECCIONAR CLIENTE VÁLIDO
    #
    # Se garantiza:
    #
    # cliente.fecha_registro <= factura.fecha_hora
    #
    # También puede ser NULL para representar una venta
    # sin cliente identificado.
    # ========================================================

    customer_id = (
        get_valid_customer_id(
            invoice_date=invoice_date,
            registration_dates=registration_dates,
            customer_ids=customer_ids,
        )
    )

    # ========================================================
    # 5. NÚMERO DE FACTURA
    # ========================================================

    invoice_code = (
        f"FAC-{invoice_number:010d}"
    )

    # ========================================================
    # 6. MÉTODO DE PAGO
    # ========================================================

    payment_method = random.choice(
        PAYMENT_METHODS
    )

    return (
        invoice_code,
        customer_id,
        store_id,
        seller_id,
        invoice_date,
        payment_method,
    )


def insert_invoice_batch(
    conn: connection,
    invoices: list[tuple],
) -> list[int]:

    query = """
        INSERT INTO facturas (
            numero_factura,
            cliente_id,
            sucursal_id,
            vendedor_id,
            fecha_hora,
            metodo_de_pago,
            subtotal,
            descuento_total,
            impuesto_total,
            total,
            estado
        )
        VALUES %s
        RETURNING factura_id;
    """

    invoice_rows = []

    for invoice in invoices:

        (
            invoice_code,
            customer_id,
            store_id,
            seller_id,
            invoice_date,
            payment_method,
        ) = invoice

        invoice_rows.append(
            (
                invoice_code,
                customer_id,
                store_id,
                seller_id,
                invoice_date,
                payment_method,
                0,
                0,
                0,
                0,
                "PAGADA",
            )
        )

    with conn.cursor() as cursor:

        returned_rows = execute_values(
            cursor,
            query,
            invoice_rows,
            page_size=len(invoice_rows),
            fetch=True,
        )

    return [
        row[0]
        for row in returned_rows
    ]


def generate_invoices(
    conn: connection,
    customer_ids: list[int],
    store_ids: list[int],
    total_invoices: int = TOTAL_INVOICES,
    batch_size: int = BATCH_SIZE,
) -> list[int]:

    with conn.cursor() as cursor:

        cursor.execute(
            "SELECT COUNT(*) FROM facturas;"
        )

        existing_count = (
            cursor.fetchone()[0]
        )

    if existing_count > 0:

        with conn.cursor() as cursor:

            cursor.execute(
                """
                SELECT factura_id
                FROM facturas
                ORDER BY factura_id;
                """
            )

            invoice_ids = [
                row[0]
                for row in cursor.fetchall()
            ]

        print(
            f"Facturas omitidas: ya existen "
            f"{existing_count} registros."
        )

        return invoice_ids

    # ========================================================
    # 1. CARGAR VENDEDORES Y SUS FECHAS
    # ========================================================

    sellers_by_store = (
        get_sellers_by_store(
            conn
        )
    )

    # ========================================================
    # 2. CARGAR CLIENTES Y SUS FECHAS DE REGISTRO
    #
    # Aunque generate_invoices recibe customer_ids para
    # mantener la interfaz existente, volvemos a consultar
    # PostgreSQL porque necesitamos fecha_registro.
    # ========================================================

    (
        registration_dates,
        valid_customer_ids,
    ) = get_customers_with_registration(
        conn
    )

    if not valid_customer_ids:

        raise ValueError(
            "No existen clientes disponibles "
            "para generar facturas."
        )

    # ========================================================
    # 3. VALIDAR SUCURSALES CON VENDEDORES
    # ========================================================

    stores_without_sellers = [
        store_id
        for store_id in store_ids
        if store_id not in sellers_by_store
    ]

    if stores_without_sellers:

        raise ValueError(
            f"Existen sucursales sin vendedores: "
            f"{stores_without_sellers}"
        )

    invoice_ids = []

    inserted_invoices = 0

    # ========================================================
    # 4. GENERAR FACTURAS POR LOTES
    # ========================================================

    for batch_start in range(
        0,
        total_invoices,
        batch_size,
    ):

        current_batch_size = min(
            batch_size,
            total_invoices - batch_start,
        )

        invoices = [
            build_invoice(
                invoice_number=(
                    batch_start
                    + index
                    + 1
                ),
                store_ids=store_ids,
                sellers_by_store=sellers_by_store,
                registration_dates=registration_dates,
                customer_ids=valid_customer_ids,
            )
            for index in range(
                current_batch_size
            )
        ]

        new_ids = insert_invoice_batch(
            conn=conn,
            invoices=invoices,
        )

        invoice_ids.extend(
            new_ids
        )

        inserted_invoices += (
            len(new_ids)
        )

        print(
            f"Facturas insertadas: "
            f"{inserted_invoices}/"
            f"{total_invoices}"
        )

    return invoice_ids