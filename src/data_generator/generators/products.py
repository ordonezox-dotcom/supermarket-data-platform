import random

from faker import Faker
from psycopg2.extensions import connection
from psycopg2.extras import execute_values

from generators.settings import BATCH_SIZE, TOTAL_PRODUCTS


fake = Faker("es_CO")

random.seed(42)
Faker.seed(42)


PRODUCT_CATALOG = {
    "Alimentos": {
        "Arroz y granos": [
            "Diana",
            "Roa",
            "Flor Huila",
        ],
        "Pastas": [
            "Doria",
            "La Muñeca",
            "Barilla",
        ],
        "Enlatados": [
            "Van Camps",
            "Zenú",
            "La Constancia",
        ],
    },

    "Bebidas": {
        "Gaseosas": [
            "Coca-Cola",
            "Pepsi",
            "Postobón",
        ],
        "Jugos": [
            "Hit",
            "Del Valle",
            "Tutti Frutti",
        ],
        "Agua": [
            "Cristal",
            "Brisa",
            "Manantial",
        ],
    },

    "Aseo": {
        "Detergentes": [
            "Ariel",
            "Fab",
            "Tide",
        ],
        "Jabones": [
            "Dove",
            "Protex",
            "Palmolive",
        ],
    },

    "Lácteos": {
        "Leche": [
            "Alpina",
            "Colanta",
            "Alquería",
        ],
        "Yogurt": [
            "Alpina",
            "Colanta",
            "Yox",
        ],
        "Quesos": [
            "Alpina",
            "Colanta",
            "Del Vecchio",
        ],
    },
}

def build_product(product_number: int) -> tuple:
    """Construye un producto sintético."""

    category = random.choice(
        list(PRODUCT_CATALOG.keys())
    )

    subcategories = PRODUCT_CATALOG[category]

    subcategory = random.choice(
        list(subcategories.keys())
    )

    brand = random.choice(
        subcategories[subcategory]
    )

    barcode = f"770{product_number:010d}"

    product_name = (
        f"{subcategory} {brand} "
        f"{product_number}"
    )

    cost = round(
        random.uniform(1_000, 80_000),
        2,
    )

    margin = random.uniform(
        1.10,
        1.60,
    )

    sale_price = round(
        cost * margin,
        2,
    )

    active = random.random() >= 0.03

    return (
        barcode,
        product_name,
        category,
        subcategory,
        brand,
        sale_price,
        cost,
        active,
    )

def insert_product_batch(
    conn: connection,
    products: list[tuple],
) -> None:

    query = """
        INSERT INTO productos (
            codigo_barras,
            nombre,
            categoria,
            subcategoria,
            marca,
            precio_venta,
            costo_unitario,
            activo
        )
        VALUES %s;
    """

    with conn.cursor() as cursor:
        execute_values(
            cursor,
            query,
            products,
            page_size=len(products),
        )

def generate_products(
    conn: connection,
    total_products: int = TOTAL_PRODUCTS,
    batch_size: int = BATCH_SIZE,
) -> list[int]:

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM productos;"
        )

        existing_count = cursor.fetchone()[0]

    if existing_count > 0:

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT producto_id
                FROM productos
                ORDER BY producto_id;
                """
            )

            product_ids = [
                row[0]
                for row in cursor.fetchall()
            ]

        print(
            f"Productos omitidos: ya existen "
            f"{existing_count} registros."
        )

        return product_ids

    inserted_products = 0

    for batch_start in range(
        0,
        total_products,
        batch_size,
    ):

        current_batch_size = min(
            batch_size,
            total_products - batch_start,
        )

        products = [
            build_product(
                batch_start + index + 1
            )
            for index in range(
                current_batch_size
            )
        ]

        insert_product_batch(
            conn=conn,
            products=products,
        )

        inserted_products += len(products)

        print(
            f"Productos insertados: "
            f"{inserted_products}/"
            f"{total_products}"
        )

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT producto_id
            FROM productos
            ORDER BY producto_id;
            """
        )

        product_ids = [
            row[0]
            for row in cursor.fetchall()
        ]

    print(
        f"Total productos disponibles: "
        f"{len(product_ids)}"
    )

    return product_ids