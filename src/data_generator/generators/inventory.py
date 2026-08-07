import random

from psycopg2.extensions import connection
from psycopg2.extras import execute_values

from generators.settings import BATCH_SIZE


random.seed(42)


def build_inventory_row(
    store_id: int,
    product_id: int,
) -> tuple:
    """Construye un registro sintético de inventario."""

    minimum_stock = random.randint(5, 30)

    maximum_stock = random.randint(
        minimum_stock + 20,
        minimum_stock + 150,
    )

    available_quantity = random.randint(
        0,
        maximum_stock,
    )

    return (
        store_id,
        product_id,
        available_quantity,
        minimum_stock,
        maximum_stock,
    )

def insert_inventory_batch(
    conn: connection,
    inventory_rows: list[tuple],
) -> None:

    query = """
        INSERT INTO inventario (
            sucursal_id,
            producto_id,
            cantidad_disponible,
            stock_minimo,
            stock_maximo
        )
        VALUES %s;
    """

    with conn.cursor() as cursor:
        execute_values(
            cursor,
            query,
            inventory_rows,
            page_size=len(inventory_rows),
        )

def generate_inventory(
    conn: connection,
    store_ids: list[int],
    product_ids: list[int],
    batch_size: int = BATCH_SIZE,
) -> int:

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM inventario;"
        )

        existing_count = cursor.fetchone()[0]

    if existing_count > 0:
        print(
            f"Inventario omitido: ya existen "
            f"{existing_count} registros."
        )

        return existing_count

    inventory_batch = []
    inserted_rows = 0

    for store_id in store_ids:
        for product_id in product_ids:

            inventory_batch.append(
                build_inventory_row(
                    store_id=store_id,
                    product_id=product_id,
                )
            )

            if len(inventory_batch) >= batch_size:

                insert_inventory_batch(
                    conn=conn,
                    inventory_rows=inventory_batch,
                )

                inserted_rows += len(inventory_batch)

                print(
                    f"Inventario insertado: "
                    f"{inserted_rows}"
                )

                inventory_batch = []

    if inventory_batch:

        insert_inventory_batch(
            conn=conn,
            inventory_rows=inventory_batch,
        )

        inserted_rows += len(inventory_batch)

    print(
        f"Total registros de inventario: "
        f"{inserted_rows}"
    )

    return inserted_rows