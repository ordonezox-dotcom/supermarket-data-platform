import random
import unicodedata

from faker import Faker
from psycopg2.extensions import connection
from psycopg2.extras import execute_values

from generators.settings import BATCH_SIZE, TOTAL_CUSTOMERS


fake = Faker("es_CO")

random.seed(42)
Faker.seed(42)


CITIES = [
    "Cali",
    "Bogotá",
    "Medellín",
    "Barranquilla",
    "Cartagena",
    "Pereira",
    "Manizales",
    "Bucaramanga",
]


DOCUMENT_TYPES = [
    "CC",
    "CE",
    "PAS",
]


def normalize_text(value: str) -> str:
    """Elimina tildes, espacios y caracteres no apropiados para correos."""

    normalized = unicodedata.normalize("NFKD", value)

    without_accents = "".join(
        character
        for character in normalized
        if not unicodedata.combining(character)
    )

    return (
        without_accents
        .lower()
        .replace(" ", "")
        .replace("'", "")
    )


def build_customer(customer_number: int) -> tuple:
    """Construye los datos de un cliente sintético."""

    first_name = fake.first_name()
    last_name = fake.last_name()

    document_type = random.choices(
        population=DOCUMENT_TYPES,
        weights=[90, 7, 3],
        k=1,
    )[0]

    document_number = f"{customer_number:010d}"

    email_name = normalize_text(first_name)
    email_last_name = normalize_text(last_name)

    email = (
        f"{email_name}.{email_last_name}."
        f"{customer_number}@example.com"
    )

    phone = fake.numerify(text="3#########")
    city = random.choice(CITIES)

    birth_date = fake.date_of_birth(
        minimum_age=18,
        maximum_age=85,
    )

    registration_date = fake.date_time_between(
        start_date="-5y",
        end_date="now",
    )

    active = random.random() >= 0.05

    return (
        document_type,
        document_number,
        first_name,
        last_name,
        email,
        phone,
        city,
        birth_date,
        registration_date,
        active,
    )


def insert_customer_batch(
    conn: connection,
    customers: list[tuple],
    ) -> None:
    
    """Inserta un lote de clientes en PostgreSQL."""

    query = """
        INSERT INTO clientes (
            tipo_documento,
            numero_documento,
            nombre,
            apellido,
            correo,
            telefono,
            ciudad,
            fecha_nacimiento,
            fecha_registro,
            activo
        )
        VALUES %s;
    """

    with conn.cursor() as cursor:
        execute_values(
            cursor,
            query,
            customers,
            page_size=len(customers),
        )


def generate_customers(
    conn: connection,
    total_customers: int = TOTAL_CUSTOMERS,
    batch_size: int = BATCH_SIZE,
) -> list[int]:
    """Genera clientes por lotes y devuelve sus identificadores."""

    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM clientes;")
        existing_count = cursor.fetchone()[0]

    if existing_count > 0:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT cliente_id
                FROM clientes
                ORDER BY cliente_id;
                """
            )
            customer_ids = [row[0] for row in cursor.fetchall()]

        print(
            f"Clientes omitidos: ya existen "
            f"{existing_count} registros."
        )

        return customer_ids

    inserted_customers = 0

    for batch_start in range(
        0,
        total_customers,
        batch_size,
                            ):
        
        current_batch_size = min(
            batch_size,
            total_customers - batch_start,
        )

        customers = [
            build_customer(batch_start + index + 1)
            for index in range(current_batch_size)
        ]

        insert_customer_batch(
            conn=conn,
            customers=customers,
        )

        inserted_customers += len(customers)

        print(
            f"Clientes insertados: "
            f"{inserted_customers}/{total_customers}"
        )

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT cliente_id
            FROM clientes
            ORDER BY cliente_id;
            """
        )
        customer_ids = [row[0] for row in cursor.fetchall()]

    print(f"Total de clientes disponibles: {len(customer_ids)}")

    return customer_ids