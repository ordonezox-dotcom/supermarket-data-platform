FROM apache/spark:3.5.1

USER root

RUN mkdir -p /home/spark/.ivy2/cache \
    && mkdir -p /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark/.ivy2

USER spark