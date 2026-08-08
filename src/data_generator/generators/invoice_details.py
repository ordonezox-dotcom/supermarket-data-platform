import random
from decimal import Decimal
from psycopg2.extensions import connection
from psycopg2.extras import execute_values

from generators.settings import BATCH_SIZE

random.seed(42)

def get_product_prices(
    conn: connection,
) -> dict[int, Decimal]:
    """Obtiene los productos activos y sus precios."""

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                producto_id,
                precio_venta
            FROM productos
            WHERE activo = TRUE;
            """
        )

        rows = cursor.fetchall()

    return {
        product_id: price
        for product_id, price in rows
    }

def build_invoice_details(
    invoice_id: int,
    product_prices: dict[int, Decimal],
) -> tuple[list[tuple], Decimal, Decimal, Decimal, Decimal]:
    """Genera las líneas y los totales de una factura."""

    number_of_products = random.randint(1, 8)

    selected_product_ids = random.sample(
        list(product_prices.keys()),
        k=number_of_products,
    )

    detail_rows = []

    subtotal = Decimal("0.00")
    total_discount = Decimal("0.00")
    total_tax = Decimal("0.00")
    invoice_total = Decimal("0.00")

    for product_id in selected_product_ids:

        quantity = random.randint(1, 5)

        unit_price = product_prices[product_id]

        if random.random() < 0.20:
            discount_percentage = Decimal(
                str(random.choice([0.05, 0.10, 0.15, 0.20]))
            )
        else:
            discount_percentage = Decimal("0.00")

        unit_discount = (
            unit_price * discount_percentage
        ).quantize(Decimal("0.01"))

        price_after_discount = (
            unit_price - unit_discount
        )

        tax_percentage = Decimal("0.19")

        unit_tax = (
            price_after_discount
            * tax_percentage
        ).quantize(Decimal("0.01"))

        line_subtotal = (
            unit_price * quantity
        ).quantize(Decimal("0.01"))

        line_discount = (
            unit_discount * quantity
        ).quantize(Decimal("0.01"))

        line_tax = (
            unit_tax * quantity
        ).quantize(Decimal("0.01"))

        line_total = (
            line_subtotal
            - line_discount
            + line_tax
        ).quantize(Decimal("0.01"))

        detail_rows.append(
            (
                invoice_id,
                product_id,
                quantity,
                unit_price,
                unit_discount,
                unit_tax,
                line_total,
            )
        )

        subtotal += line_subtotal
        total_discount += line_discount
        total_tax += line_tax
        invoice_total += line_total

    return (
        detail_rows,
        subtotal,
        total_discount,
        total_tax,
        invoice_total,
    )

def insert_detail_batch(
    conn: connection,
    detail_rows: list[tuple],
) -> None:

    query = """
        INSERT INTO detalles_factura (
            factura_id,
            producto_id,
            cantidad,
            precio_unitario,
            descuento_unitario,
            impuesto_unitario,
            total_linea
        )
        VALUES %s;
    """

    with conn.cursor() as cursor:
        execute_values(
            cursor,
            query,
            detail_rows,
            page_size=BATCH_SIZE,
        )

def update_invoice_totals(
    conn: connection,
    invoice_totals: list[tuple],
) -> None:

    query = """
        UPDATE facturas AS f
        SET
            subtotal = v.subtotal,
            descuento_total = v.descuento,
            impuesto_total = v.impuesto,
            total = v.total
        FROM (
            VALUES %s
        ) AS v(
            factura_id,
            subtotal,
            descuento,
            impuesto,
            total
        )
        WHERE f.factura_id = v.factura_id;
    """

    with conn.cursor() as cursor:
        execute_values(
            cursor,
            query,
            invoice_totals,
            page_size=BATCH_SIZE,
        )

def generate_invoice_details(
    conn: connection,
    invoice_ids: list[int],
    batch_size: int = BATCH_SIZE,
) -> int:

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM detalles_factura;"
        )
        existing_count = cursor.fetchone()[0]

    if existing_count > 0:

        print(
            f"Detalles omitidos: ya existen "
            f"{existing_count} registros."
        )

        return existing_count

    product_prices = get_product_prices(conn)

    total_details = 0

    for batch_start in range(
        0,
        len(invoice_ids),
        batch_size,
    ):

        invoice_batch = invoice_ids[
            batch_start:
            batch_start + batch_size
        ]

        detail_batch = []
        invoice_totals = []

        for invoice_id in invoice_batch:

            (
                details,
                subtotal,
                discount,
                tax,
                total,
            ) = build_invoice_details(
                invoice_id=invoice_id,
                product_prices=product_prices,
            )

            detail_batch.extend(details)

            invoice_totals.append(
                (
                    invoice_id,
                    subtotal,
                    discount,
                    tax,
                    total,
                )
            )

        insert_detail_batch(
            conn=conn,
            detail_rows=detail_batch,
        )

        update_invoice_totals(
            conn=conn,
            invoice_totals=invoice_totals,
        )

        total_details += len(detail_batch)

        print(
            f"Facturas procesadas: "
            f"{min(batch_start + batch_size, len(invoice_ids))}"
            f"/{len(invoice_ids)} | "
            f"Detalles: {total_details}"
        )

    print(
        f"Total detalles de factura: "
        f"{total_details}"
    )

    return total_details
