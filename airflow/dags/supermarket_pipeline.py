from datetime import datetime
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# ============================================================
# ZONA HORARIA
# ============================================================

TIMEZONE = pendulum.timezone("America/Bogota")

with DAG(
    dag_id="supermarket_pipeline",
    description="Pipeline ETL del supermercado: Bronze -> Silver -> Gold",
    start_date=pendulum.datetime(
        2026,
        9,
        1,
        2,
        0,
        tz=TIMEZONE,
    ),
    schedule="0 2 * * *",
    catchup=False,
    tags=["supermarket", "spark", "etl"],
) as dag:

    # =========================================================
    # BRONZE
    # PostgreSQL -> Bronze Delta
    # =========================================================

    extract_bronze = SparkSubmitOperator(
        task_id="extract_bronze",
        application="/opt/airflow/src/spark/bronze/extract_all_tables_delta.py",
        conn_id="spark_default",
    )


    # =========================================================
    # SILVER
    # Bronze Delta -> Silver Delta
    # =========================================================

    transform_silver = SparkSubmitOperator(
        task_id="transform_silver",
        application="/opt/airflow/src/spark/silver/transform_all_tables.py",
        conn_id="spark_default",
    )


    # =========================================================
    # GOLD - DIM CLIENTE
    # =========================================================

    update_dim_cliente = SparkSubmitOperator(
        task_id="update_dim_cliente",
        application="/opt/airflow/src/spark/gold/incremental/update_dim_cliente.py",
        conn_id="spark_default",
    )


    # =========================================================
    # GOLD - DIM PRODUCTO
    # =========================================================

    update_dim_producto = SparkSubmitOperator(
        task_id="update_dim_producto",
        application="/opt/airflow/src/spark/gold/incremental/update_dim_producto.py",
        conn_id="spark_default",
    )


    # =========================================================
    # GOLD - DIM SUCURSAL
    # =========================================================

    update_dim_sucursal = SparkSubmitOperator(
        task_id="update_dim_sucursal",
        application="/opt/airflow/src/spark/gold/incremental/update_dim_sucursal.py",
        conn_id="spark_default",
    )


    # =========================================================
    # GOLD - DIM VENDEDOR
    # =========================================================

    update_dim_vendedor = SparkSubmitOperator(
        task_id="update_dim_vendedor",
        application="/opt/airflow/src/spark/gold/incremental/update_dim_vendedor.py",
        conn_id="spark_default",
    )


    # =========================================================
    # GOLD - DIM FECHA
    # =========================================================

    update_dim_fecha = SparkSubmitOperator(
        task_id="update_dim_fecha",
        application="/opt/airflow/src/spark/gold/incremental/update_dim_fecha.py",
        conn_id="spark_default",
    )


    # =========================================================
    # GOLD - DIM CONTEXTO DE VENTA
    # =========================================================

    update_dim_contexto_venta = SparkSubmitOperator(
        task_id="update_dim_contexto_venta",
        application="/opt/airflow/src/spark/gold/incremental/update_dim_contexto_venta.py",
        conn_id="spark_default",
    )


    # =========================================================
    # QUALITY
    # Validación de facturas / Quarantine
    # =========================================================

    validate_invoices = SparkSubmitOperator(
        task_id="validate_invoices",
        application="/opt/airflow/src/spark/gold/quality/validate_invoices.py",
        conn_id="spark_default",
    )


    # =========================================================
    # GOLD - FACT VENTAS
    #
    # Se ejecuta después de actualizar las dimensiones.
    # =========================================================

    update_fact_ventas = SparkSubmitOperator(
        task_id="update_fact_ventas",
        application="/opt/airflow/src/spark/gold/incremental/update_fact_ventas.py",
        conn_id="spark_default",
    )


    # =========================================================
    # DEPENDENCIAS DEL PIPELINE
    # =========================================================

    (
        extract_bronze
        >> transform_silver
        >> update_dim_cliente
        >> update_dim_producto
        >> update_dim_sucursal
        >> update_dim_vendedor
        >> update_dim_fecha
        >> update_dim_contexto_venta
        >> validate_invoices
        >> update_fact_ventas
    )