from database import get_connection

from generators.customers import generate_customers
from generators.sellers import generate_sellers
from generators.stores import generate_stores
from generators.products import generate_products
from generators.inventory import generate_inventory
from generators.invoices import generate_invoices
from generators.invoice_details import generate_invoice_details


def generate_all_data() -> None:
    """Genera todo el dataset operacional del supermercado."""

    with get_connection() as conn:

        # ====================================================
        # 1. SUCURSALES
        # ====================================================

        store_ids = generate_stores(
            conn
        )

        # ====================================================
        # 2. VENDEDORES
        #
        # Cada vendedor queda asociado a una sucursal.
        # ====================================================

        seller_ids = generate_sellers(
            conn=conn,
            store_ids=store_ids,
            sellers_per_store=5,
        )

        # ====================================================
        # 3. CLIENTES
        # ====================================================

        customer_ids = generate_customers(
            conn
        )

        # ====================================================
        # 4. PRODUCTOS
        # ====================================================

        product_ids = generate_products(
            conn
        )

        # ====================================================
        # 5. INVENTARIO
        #
        # Generamos una combinación sucursal-producto.
        # ====================================================

        inventory_count = generate_inventory(
            conn=conn,
            store_ids=store_ids,
            product_ids=product_ids,
        )

        # ====================================================
        # 6. FACTURAS
        #
        # invoices.py se encarga de garantizar:
        #
        # cliente.fecha_registro <= factura.fecha_hora
        #
        # vendedor.fecha_de_contratacion
        #     <= factura.fecha_hora
        #
        # vendedor.sucursal_id
        #     == factura.sucursal_id
        # ====================================================

        invoice_ids = generate_invoices(
            conn=conn,
            customer_ids=customer_ids,
            store_ids=store_ids,
        )

        # ====================================================
        # 7. DETALLES DE FACTURA
        #
        # Cada factura recibe entre 1 y 8 productos
        # y sus totales son actualizados en facturas.
        # ====================================================

        detail_count = generate_invoice_details(
            conn=conn,
            invoice_ids=invoice_ids,
        )

    # ========================================================
    # RESUMEN
    # ========================================================

    print()
    print("Generación finalizada.")
    print(
        f"Sucursales disponibles: "
        f"{len(store_ids)}"
    )
    print(
        f"Vendedores disponibles: "
        f"{len(seller_ids)}"
    )
    print(
        f"Clientes disponibles: "
        f"{len(customer_ids)}"
    )
    print(
        f"Productos disponibles: "
        f"{len(product_ids)}"
    )
    print(
        f"Registros de inventario: "
        f"{inventory_count}"
    )
    print(
        f"Facturas generadas: "
        f"{len(invoice_ids)}"
    )
    print(
        f"Detalles de factura: "
        f"{detail_count}"
    )


if __name__ == "__main__":
    generate_all_data()