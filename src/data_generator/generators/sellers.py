import random

from faker import Faker
from psycopg2.extensions import connection

fake = Faker("es_CO")
random.seed(42)
Faker.seed(42)


def generate_sellers(
        conn: connection,
        store_ids: list[int],
        sellers_per_store: int = 5,
) -> list[int]:
    """Genera vendedores asociados a las sucursales """
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM vendedores;")
        existing_count = cursor.fetchone()[0]

        if existing_count > 0:
            cursor.execute(
                """ 
                SELECT vendedor_id
                FROM vendedores
                ORDER BY vendedor_id;
                """
            )
            seller_ids = [row[0] for row in cursor.fetchall()]
            print(
                f"vendedores omitidos: ya esxistentes "
                f"{existing_count} registros"
            )
            return seller_ids

        sellers =[]

        for store_id in store_ids:
            for _ in range(sellers_per_store):
                first_name = fake.first_name()
                last_name = fake.last_name()

                document = fake.unique.numerify(
                    text = "##########"
                )

                email = (
                    f"{first_name}.{last_name}"
                    f"{random.randint(1,9999)}"
                    "@supermarket.com"
                ).lower().replace(" ","")

                hire_date = fake.date_between(
                    start_date = "-10y",
                    end_date = "-30d"
                )

                sellers.append(
                    (
                    store_id,
                    document,
                    first_name,
                    last_name,
                    email,
                    hire_date,
                    )
                )

        cursor.executemany(
             """
             INSERT INTO vendedores (
                 sucursal_id,
                 documento,
                 nombre,
                 apellidos,
                 correo,
                 fecha_de_contratacion
             )
             VALUES (%s, %s, %s, %s, %s, %s);
             """,
             sellers,
         )
 
        cursor.execute(
             """
             SELECT vendedor_id
             FROM vendedores
             ORDER BY vendedor_id;
             """
         )
        seller_ids = [row[0] for row in cursor.fetchall()]
 
    print(f"Vendedores insertados: {len(seller_ids)}")
    return seller_ids              
                
