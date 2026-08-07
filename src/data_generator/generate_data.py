from database import get_connection
from generators.customers import generate_customers
from generators.sellers import generate_sellers
from generators.stores import generate_stores
from generators.products import generate_products
from generators.inventory import generate_inventory


def generate_all_data() -> None:
    with get_connection() as conn:
        store_ids = generate_stores(conn)

        seller_ids = generate_sellers(
            conn=conn,
            store_ids=store_ids,
            sellers_per_store=5,
        )

        customer_ids = generate_customers(conn)
        product_ids = generate_products(conn)

        inventory_count = generate_inventory(
            conn=conn,
            store_ids=store_ids,
            product_ids=product_ids,
        )

    print("\nGeneración finalizada.")
    print(f"Sucursales disponibles: {len(store_ids)}")
    print(f"Vendedores disponibles: {len(seller_ids)}")
    print(f"Clientes disponibles: {len(customer_ids)}")
    print(f"Productos disponibles: "f"{len(product_ids)}")
    print(f"Registros de inventario: "f"{inventory_count}")


if __name__ == "__main__":
    generate_all_data()