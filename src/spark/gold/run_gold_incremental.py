import subprocess
import sys
from datetime import datetime


SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"

GOLD_BASE = "/opt/spark-apps/spark/gold"


PIPELINE = [

    # ========================================================
    # DIMENSIONES INCREMENTALES
    # ========================================================

    (
        "DIM_CLIENTE",
        f"{GOLD_BASE}/incremental/update_dim_cliente.py",
    ),

    (
        "DIM_PRODUCTO",
        f"{GOLD_BASE}/incremental/update_dim_producto.py",
    ),

    (
        "DIM_SUCURSAL",
        f"{GOLD_BASE}/incremental/update_dim_sucursal.py",
    ),

    (
        "DIM_VENDEDOR",
        f"{GOLD_BASE}/incremental/update_dim_vendedor.py",
    ),

    (
        "DIM_FECHA",
        f"{GOLD_BASE}/incremental/update_dim_fecha.py",
    ),

    (
        "DIM_CONTEXTO_VENTA",
        f"{GOLD_BASE}/incremental/update_dim_contexto_venta.py",
    ),

    # ========================================================
    # QUALITY / QUARANTINE
    # ========================================================

    (
        "VALIDATE_INVOICES",
        f"{GOLD_BASE}/quality/validate_invoices.py",
    ),

    # ========================================================
    # FACT
    #
    # Siempre debe ejecutarse después:
    #
    # 1. dimensiones
    # 2. validaciones
    # 3. quarantine
    # ========================================================

    (
        "FACT_VENTAS",
        f"{GOLD_BASE}/incremental/update_fact_ventas.py",
    ),
]


def run_spark_job(name, script_path):

    print("\n")
    print("=" * 70)
    print(f"INICIANDO: {name}")
    print("=" * 70)

    start_time = datetime.now()

    command = [
        SPARK_SUBMIT,
        "--master",
        SPARK_MASTER,
        script_path,
    ]

    print(
        f"\nScript: {script_path}"
    )

    print(
        f"Inicio: {start_time}"
    )

    result = subprocess.run(
        command
    )

    end_time = datetime.now()

    duration = (
        end_time
        -
        start_time
    )

    # ========================================================
    # FAIL FAST
    #
    # Si cualquier proceso falla:
    #
    # NO seguimos con el pipeline.
    # ========================================================

    if result.returncode != 0:

        print("\n")
        print("=" * 70)
        print(f"ERROR EN: {name}")
        print("=" * 70)

        print(
            f"Código de salida: "
            f"{result.returncode}"
        )

        print(
            f"Duración: "
            f"{duration}"
        )

        raise RuntimeError(
            f"El proceso {name} falló. "
            f"Pipeline Gold detenido."
        )

    print("\n")
    print("=" * 70)
    print(f"COMPLETADO: {name}")
    print("=" * 70)

    print(
        f"Duración: "
        f"{duration}"
    )


def run_gold_incremental():

    pipeline_start = datetime.now()

    print("\n")
    print("=" * 70)
    print("GOLD INCREMENTAL PIPELINE")
    print("=" * 70)

    print(
        f"\nInicio pipeline: "
        f"{pipeline_start}"
    )

    print(
        f"Procesos programados: "
        f"{len(PIPELINE)}"
    )

    try:

        for name, script_path in PIPELINE:

            run_spark_job(
                name,
                script_path,
            )

    except Exception as error:

        pipeline_end = datetime.now()

        print("\n")
        print("=" * 70)
        print("GOLD INCREMENTAL PIPELINE FAILED")
        print("=" * 70)

        print(
            f"\nError:"
            f"\n{error}"
        )

        print(
            f"\nDuración total: "
            f"{pipeline_end - pipeline_start}"
        )

        sys.exit(1)

    pipeline_end = datetime.now()

    print("\n")
    print("=" * 70)
    print("GOLD INCREMENTAL PIPELINE COMPLETADO")
    print("=" * 70)

    print(
        f"\nInicio: "
        f"{pipeline_start}"
    )

    print(
        f"Fin: "
        f"{pipeline_end}"
    )

    print(
        f"Duración total: "
        f"{pipeline_end - pipeline_start}"
    )

    print(
        "\nTodos los procesos Gold "
        "terminaron correctamente."
    )


if __name__ == "__main__":
    run_gold_incremental()