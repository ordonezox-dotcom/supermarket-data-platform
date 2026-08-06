from psycopg2.extensions import connection

STORES = [
    (
        "Supermercado Chipichape",
        "Cali",
        "Avenida 6N # 37N-25",
        "2010-03-15",
    ),
    (
        "Supermercado Unicentro",
        "Cali",
        "Carrera 100 # 5-169",
        "2012-07-20",
    ),
    (
        "Supermercado San Fernando",
        "Cali",
        "Calle 5 # 38D-35",
        "2015-01-10",
    ),
    (
        "Supermercado La Flora",
        "Cali",
        "Avenida 6N # 47-02",
        "2018-09-05",
    ),
    (
        "Supermercado Centro",
        "Bogotá",
        "Carrera 7 # 20-15",
        "2008-05-12",
    ),
    (
        "Supermercado Suba",
        "Bogotá",
        "Avenida Suba # 104-25",
        "2014-11-18",
    ),
    (
        "Supermercado El Poblado",
        "Medellín",
        "Carrera 43A # 10-45",
        "2011-06-30",
    ),
    (
        "Supermercado Laureles",
        "Medellín",
        "Circular 5 # 70-20",
        "2016-04-22",
    ),
    (
        "Supermercado Buenavista",
        "Barranquilla",
        "Carrera 53 # 98-50",
        "2017-08-14",
    ),
    (
        "Supermercado Bocagrande",
        "Cartagena",
        "Carrera 2 # 8-25",
        "2019-02-28",
    ),
]

def generate_stores(conn: connection) -> list[int]:
    """ Inserta sucursales y devuelve sus identificadores """

    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM sucursales;")
        existing_count = cursor.fetchone()[0]

        if existing_count > 0:
            cursor.execute(
                """ 
                SELECT sucursal_id
                FROM sucursales
                ORDER BY sucursal_id
                """
            )
            stores_ids = [row[0] for row in cursor.fetchall()]
            print(
                f"Sucursales omitidas: ya existen"
                f"{existing_count} registros"
            )
            return stores_ids
        cursor.executemany(
            """
            INSERT INTO sucursales (
                nombre,
                ciudad,
                direccion,
                fecha_apertura
            )
            VALUES (%s, %s, %s, %s);
            """,
            STORES,
        )

        cursor.execute(
            """
            SELECT sucursal_id
            FROM sucursales
            ORDER BY sucursal_id;
            """
        )
        store_ids = [row[0] for row in cursor.fetchall()]

    print(f"Sucursales insertadas: {len(store_ids)}")
    return store_ids