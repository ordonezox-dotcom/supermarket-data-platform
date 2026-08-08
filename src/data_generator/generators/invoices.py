import random
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

def build_invoice (
        invoice_number: int,
        customer_ids: list[int],
        store_ids: list[int],
        seller_by_store: dict[int, list[int]],
) -> tuple:
    """Costruye unua factura sintetisada """

    store_id = random.choice(store_ids)
    seller_id = random.choice(seller_by_store[store_id])
    customer_id = random.choice(customer_ids + [None])

    invoice_code = (
        f"FAC--{invoice_number:010d}"
    )

    invoice_date = faker.date_time_between(
        start_date = "-2y",
        end_date = "now",
    )

    payment_method = random.choice(PAYMENT_METHODS)
    return (
        invoice_code,
        customer_id,
        store_id,
        seller_id,
        invoice_date,
        payment_method,
    )

def get_sellers_by_store(
        conn: connection,
) -> dict[int, list[int]]:
    """Agrupa vendedores por sucursal """
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                sucursal_id,
                vendedor_id
            FROM vendedores
            ORDER BY sucursal_id;
            """
        )

        rows = cursor.fetchall()

    sellers_by_store = {}

    for store_id, seller_id in rows:
        sellers_by_store.setdefault(store_id,[]).append(seller_id)

    return sellers_by_store


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
        existing_count = cursor.fetchone()[0]

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

    sellers_by_store = get_sellers_by_store(
        conn
    )

    invoice_ids = []
    inserted_invoices = 0

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
                    batch_start + index + 1
                ),
                customer_ids=customer_ids,
                store_ids=store_ids,
                seller_by_store=sellers_by_store,
            )
            for index in range(
                current_batch_size
            )
        ]

        new_ids = insert_invoice_batch(
            conn=conn,
            invoices=invoices,
        )

        invoice_ids.extend(new_ids)

        inserted_invoices += len(new_ids)

        print(
            f"Facturas insertadas: "
            f"{inserted_invoices}/"
            f"{total_invoices}"
        )

    return invoice_ids

