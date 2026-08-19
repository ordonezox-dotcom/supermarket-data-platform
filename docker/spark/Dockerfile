FROM apache/spark:3.5.3

USER root

# Versiones fijas para que el entorno sea reproducible
ARG POSTGRES_JDBC_VERSION=42.7.7
ARG DELTA_VERSION=3.3.2

# Preparar la carpeta de Ivy
RUN mkdir -p /home/spark/.ivy2/cache \
    && mkdir -p /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark/.ivy2

# Instalar la API Python de Delta Lake
RUN pip install --no-cache-dir delta-spark==${DELTA_VERSION}

# PostgreSQL JDBC
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Delta Lake JARs
ADD https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar \
    /opt/spark/jars/delta-spark_2.12-${DELTA_VERSION}.jar

ADD https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
    /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar

# Permisos
RUN chown spark:spark \
    /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /opt/spark/jars/delta-spark_2.12-${DELTA_VERSION}.jar \
    /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar

USER spark