# ============================================================
# ETAPA 1
# Imagen Spark del proyecto.
#
# La utilizamos como fuente de los mismos JAR utilizados
# por Spark Master y Spark Worker.
# ============================================================

FROM supermarket-spark:3.5.1 AS spark-jars


# ============================================================
# ETAPA 2
# Imagen final de Airflow
# ============================================================

FROM apache/airflow:2.10.5


# ============================================================
# JAVA + HERRAMIENTAS NECESARIAS PARA SPARK
# ============================================================

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64


# ============================================================
# AIRFLOW + SPARK
#
# IMPORTANTE:
#
# Nuestro cluster utiliza Apache Spark 3.5.3.
#
# Por eso el spark-submit ejecutado desde Airflow también debe
# utilizar PySpark/Spark 3.5.3.
# ============================================================

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    "apache-airflow-providers-apache-spark==5.2.1" \
    "pyspark==3.5.3"


# ============================================================
# DELTA LAKE - API PYTHON
#
# Utilizamos exactamente Delta 3.3.2, igual que el cluster.
#
# Permite:
#
# from delta.tables import DeltaTable
# ============================================================

RUN pip install --no-cache-dir --no-deps \
    "delta-spark==3.3.2"


# ============================================================
# JARs
#
# No descargamos otra versión de Delta para Airflow.
#
# Copiamos directamente los JAR de nuestra imagen Spark.
# Así Driver, Master y Worker utilizan exactamente los mismos.
# ============================================================

USER root

COPY --from=spark-jars \
    /opt/spark/jars/delta-storage-3.3.2.jar \
    /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/

COPY --from=spark-jars \
    /opt/spark/jars/delta-spark_2.12-3.3.2.jar \
    /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/

COPY --from=spark-jars \
    /opt/spark/jars/postgresql-42.7.7.jar \
    /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/


# ============================================================
# PERMISOS
# ============================================================

RUN chown airflow:root \
    /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/delta-storage-3.3.2.jar \
    /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/delta-spark_2.12-3.3.2.jar \
    /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/postgresql-42.7.7.jar


USER airflow