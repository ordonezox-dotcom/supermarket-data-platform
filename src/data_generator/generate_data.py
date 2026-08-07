from database import get_connection
from generators.customers import generate_customers
from generators.sellers import generate_sellers
from generators.stores import generate_stores


def generate_all_data() -> None:
    with get_connection() as conn:
        store_ids = generate_stores(conn)

        seller_ids = generate_sellers(
            conn=conn,
            store_ids=store_ids,
            sellers_per_store=5,
        )

        customer_ids = generate_customers(conn)

    print("\nGeneración finalizada.")
    print(f"Sucursales disponibles: {len(store_ids)}")
    print(f"Vendedores disponibles: {len(seller_ids)}")
    print(f"Clientes disponibles: {len(customer_ids)}")


if __name__ == "__main__":
    generate_all_data()