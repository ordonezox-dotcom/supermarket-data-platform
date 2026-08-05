from database import get_connection


def test_connection() -> None:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_database(), current_user;")
            result = cursor.fetchone()

    print(f"Conexión correcta: base={result[0]}, usuario={result[1]}")


if __name__ == "__main__":
    test_connection()