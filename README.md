Supermarket Data Platform

End-to-end Data Engineering portfolio project that simulates a
supermarket data platform.

The project implements a Lakehouse-style architecture with PostgreSQL,
Apache Spark, PySpark, Delta Lake, Docker and Apache Airflow. It follows
a Medallion Architecture (Bronze, Silver and Gold) and includes
incremental processing, dimensional modeling, historical tracking and
data quality controls.

The repository is developed step by step so the architecture can be
understood and reproduced. The project is functional and will continue
receiving improvements as development advances.

Project Status

Current stage: Functional end-to-end pipeline orchestrated with Apache
Airflow

Implemented

PostgreSQL 16 operational source database.

Synthetic supermarket data generation.

Dockerized and reproducible development environment.

Spark Standalone cluster with Master and Worker.

PostgreSQL JDBC integration with Spark.

Delta Lake storage.

Bronze, Silver and Gold layers.

Incremental ingestion and processing.

Data cleaning, normalization and technical deduplication.

Historical source-version preservation.

Dimensional sales model.

Surrogate keys.

SCD Type 1 and Type 2 dimensions.

Incremental Gold processing.

Invoice reconciliation and Quarantine dataset.

Apache Airflow orchestration.

Spark connection configured in Airflow.

End-to-end task dependencies.

Daily automatic execution at 02:00 using America/Bogota.

End-to-end execution successfully validated.

Improvements planned

The project will continue evolving. Current improvement areas include:

Pipeline retries and failure handling.

Alerts and monitoring improvements.

Performance and resource optimization.

Improve Quarantine behavior so invalid invoices are explicitly
excluded from Gold until corrected.

Analytics / dashboard layer.

Final documentation refinements.

Current Quarantine behavior: invalid invoices are detected and
recorded in Quarantine, but fact_ventas does not yet explicitly
exclude every quarantined invoice. This is a known improvement and is
not presented as completed functionality.

Architecture

                    +----------------------+
                    | PostgreSQL Source    |
                    | Operational Database |
                    +----------+-----------+
                               |
                               | Spark extraction
                               v
                    +----------------------+
                    | BRONZE - Delta Lake  |
                    | Raw / History        |
                    +----------+-----------+
                               |
                               | Spark transformation
                               v
                    +----------------------+
                    | SILVER - Delta Lake  |
                    | Clean / Standardized |
                    +----------+-----------+
                               |
                               | Spark dimensional processing
                               v
              +------------------------------------+
              | Data Quality / Invoice Validation  |
              +----------------+-------------------+
                               |
                  +------------+-------------+
                  |                          |
                  v                          v
        +------------------+       +------------------+
        | GOLD             |       | QUARANTINE       |
        | Dimensional Model|       | Invalid invoices |
        +------------------+       +------------------+

                  Apache Airflow
                        |
                        | orchestrates
                        v
        Bronze -> Silver -> Gold / Quality tasks

Airflow launches Spark applications through SparkSubmitOperator. Spark
runs in Standalone mode using a Master and Worker container. The current
environment simulates the distributed architecture on one physical
machine.

Technology Stack

Technology                          Purpose

PostgreSQL 16                       Operational source database and
Airflow metadata database

Apache Spark 3.5.3                  Distributed data processing

PySpark 3.5.3                       Spark transformations

Delta Lake 3.3.2                    ACID storage and table versioning

Apache Airflow 2.10.5               Pipeline orchestration and
scheduling

Airflow Spark Provider 5.2.1        SparkSubmitOperator integration

PostgreSQL JDBC 42.7.7              Spark/PostgreSQL connectivity

Java 17                             Spark runtime

Docker                              Reproducible execution environment

Docker Compose                      Multi-container infrastructure

Python                              Data generation and processing

Data Flow

PostgreSQL
    |
    | incremental extraction / full reload by table
    v
BRONZE - Delta
    |
    | cleaning
    | normalization
    | validation
    | technical deduplication
    | incremental processing
    v
SILVER - Delta
    |
    | dimensional transformations
    | surrogate keys
    | SCD Type 1 / Type 2
    v
GOLD - Delta
    |
    +--> Dimensions
    |
    +--> Invoice quality validation
            |
            +--> Quarantine
    |
    +--> fact_ventas

Bronze Layer

Bronze stores source data in Delta Lake while preserving source-level
history.

The extraction strategy depends on the source table:

Incremental extraction using increasing IDs.

Incremental extraction using source update timestamps.

Full reload for small tables where appropriate.

Current examples:

clientes: incremental by updated_at.

productos: incremental by updated_at.

inventario: incremental by fecha_actualizacion.

facturas: incremental by factura_id.

detalles_factura: incremental by detalle_id.

sucursales: full reload.

vendedores: full reload.

Silver Layer

Silver transforms Bronze data into cleaner and standardized datasets.

Implemented transformations include:

Required-field and null validation.

Text normalization.

Email format validation.

Numeric range validation.

Technical duplicate removal.

Incremental processing.

Preservation of meaningful source versions.

Silver does not apply dimensional SCD logic. Historical versions are
preserved so Gold can determine how changes should be modeled.

Gold Layer

Gold contains the dimensional sales model.

The modeled business process is:

Supermarket product sales

The grain of fact_ventas is:

One row represents one product line within one invoice.

Dimensions

dim_cliente
dim_producto
dim_sucursal
dim_vendedor
dim_fecha
dim_hora
dim_contexto_venta

dim_contexto_venta follows the junk-dimension pattern for small
transaction categories such as payment method and status.

numero_factura is stored directly in fact_ventas as a degenerate
dimension.

Fact table

fact_ventas

venta_sk
detalle_id
numero_factura
cliente_sk
producto_sk
sucursal_sk
vendedor_sk
fecha_sk
hora_sk
contexto_venta_sk
cantidad
precio_unitario
subtotal_linea
descuento_linea
impuesto_linea
total_linea

Slowly Changing Dimensions

The Gold layer implements SCD Type 1 and Type 2 according to the
business meaning of each attribute.

dim_cliente

SCD Type 2:

ciudad

Other mutable descriptive attributes are handled as SCD Type 1.

dim_producto

SCD Type 2:

categoria
subcategoria
marca
precio_venta
costo_unitario

Other descriptive attributes use SCD Type 1.

dim_sucursal

SCD Type 2:

ciudad
direccion

Other descriptive attributes use SCD Type 1.

dim_vendedor

SCD Type 2:

sucursal_id

Other descriptive attributes use SCD Type 1.

SCD Type 2 dimensions use technical columns such as:

fecha_inicio
fecha_fin
es_actual
source_updated_at

A new surrogate key is generated when a historical SCD Type 2 version is
created.

Data Quality and Quarantine

Data quality is applied during transformation and through explicit
business validations.

The project includes invoice reconciliation between invoice totals and
their detail lines:

SUM(subtotal_linea)  == facturas.subtotal
SUM(descuento_linea) == facturas.descuento_total
SUM(impuesto_linea)  == facturas.impuesto_total
SUM(total_linea)     == facturas.total

The validation is implemented in:

src/spark/gold/quality/validate_invoices.py

Detected invalid invoices are recorded in:

data/quarantine/facturas

This allows quality problems to be investigated instead of silently
deleting the records.

Incremental Processing

The pipeline avoids unnecessarily rebuilding all data:

Source
  |
  v
Bronze     incremental / full reload by source
  |
  v
Silver     incremental / snapshot comparison
  |
  v
Gold       incremental + SCD processing

The incremental pipeline has also been tested by executing it without
new source changes to verify that already processed records are not
unnecessarily inserted again.

Apache Airflow Orchestration

Airflow orchestrates the complete pipeline. The DAG is located at:

airflow/dags/supermarket_pipeline.py

Current execution order:

extract_bronze
      |
      v
transform_silver
      |
      v
update_dim_cliente
      |
      v
update_dim_producto
      |
      v
update_dim_sucursal
      |
      v
update_dim_vendedor
      |
      v
update_dim_fecha
      |
      v
update_dim_contexto_venta
      |
      v
validate_invoices
      |
      v
update_fact_ventas

dim_hora is static/precalculated, so it does not require a recurring
incremental task.

The DAG is scheduled daily:

0 2 * * *

Timezone:

America/Bogota

catchup=False is configured to avoid automatic historical backfill.

Airflow Spark Connection

The Spark connection is configured through the Airflow UI and is
intentionally documented because it is not stored as application code in
the repository.

After Airflow is running, create the following connection:

Admin / Connections

Connection Id:   spark_default
Connection Type: Spark
Host:            spark://spark-master
Port:            7077

The DAG uses:

conn_id="spark_default"

This allows SparkSubmitOperator to submit the Spark applications to
the configured Spark Master without hardcoding the Master address in
every task.

Reproducing the Project

The repository is intended to be followed step by step.

1. Clone the repository

git clone <repository-url>
cd supermarket-data-platform

2. Configure environment variables

Create/configure the .env values required by docker-compose.yml for
the operational PostgreSQL database and Spark source connection.

Do not commit credentials to Git.

3. Build and start the infrastructure

Docker Desktop / Docker Engine must be running.

docker compose up --build

The Compose environment starts the operational PostgreSQL database,
Spark Master/Worker, Airflow metadata PostgreSQL database, Airflow
initialization, webserver and scheduler. A one-shot permissions service
prepares the shared data directories used by Spark and Airflow.

4. Generate the synthetic source data

On Windows PowerShell, using the project's virtual environment:

.\.venv\Scripts\Activate.ps1
python .\src\data_generator\generate_data.py

5. Verify the services

Main local interfaces:

Airflow UI:       http://localhost:8080
Spark Master UI:  http://localhost:8081
Spark Worker UI:  http://localhost:8082

6. Configure the Airflow Spark connection

Create spark_default using the values documented in the Airflow
Spark Connection section.

This manual configuration is required for a fresh environment because
the DAG references that connection.

7. Run the pipeline

Enable the supermarket_pipeline DAG in Airflow.

The DAG can be triggered manually for validation and is also configured
for daily execution at 02:00 in the America/Bogota timezone.

The pipeline executes:

PostgreSQL
   -> Bronze
   -> Silver
   -> Gold dimensions
   -> Data Quality
   -> fact_ventas

8. Validate the execution

Use:

Airflow Grid/Task Logs to inspect task execution.

Spark Master/Worker UI to inspect Spark applications and cluster
resources.

Bronze, Silver, Gold and Quarantine Delta datasets to inspect
pipeline output.

Repository Structure

supermarket-data-platform/
|
├── airflow/
│   ├── dags/
│   │   └── supermarket_pipeline.py
│   └── logs/
|
├── docker/
│   ├── spark/
│   │   └── Dockerfile
│   └── airflow/
│       └── Dockerfile
|
├── docs/
│   └── data-model/
|
├── sql/
│   └── tests/
|
├── src/
│   ├── data_generator/
│   └── spark/
│       ├── bronze/
│       ├── silver/
│       └── gold/
│           ├── initial/
│           ├── incremental/
│           ├── quality/
│           └── run_gold_incremental.py
|
├── data/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── quarantine/
|
├── docker-compose.yml
├── requirements.txt
└── README.md

Project Principles

The project currently demonstrates:

Reproducible containerized infrastructure.

Incremental data processing.

Separation of raw, clean and analytical layers.

Historical traceability.

Dimensional modeling.

Surrogate keys.

SCD Type 1 and Type 2.

Data quality rules and Quarantine.

Spark distributed-processing architecture.

Airflow orchestration and scheduling.

Explicit task dependencies.

Separation between orchestration and transformation.

Git-based version control.

Project Evolution

This project is intentionally iterative. The current pipeline is
functional end to end, but improvements will continue to be implemented
and documented as the platform evolves.

The README describes functionality that has actually been implemented.
Planned improvements are kept separate so the repository can be used
both as a portfolio project and as a reproducible learning reference.

Author

Data Engineering portfolio project focused on building and understanding
an end-to-end Lakehouse data platform.