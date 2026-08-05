# Supermarket Data Platform

Proyecto de portafolio de Ingeniería de Datos que simula una plataforma analítica para una cadena de supermercados.

## Objetivo

Integrar datos operacionales de ventas, clientes, productos, sucursales e inventario, procesarlos mediante un pipeline de datos y construir un Data Warehouse dimensional para análisis.

## Arquitectura planeada

Fuentes de datos → Airflow → PySpark → Bronze/Silver → PostgreSQL → dbt → Power BI

## Granularidad principal

Cada fila de la tabla de hechos de ventas representará una línea de producto dentro de una factura.

## Estado actual

Creación inicial del repositorio y configuración de las bases de datos PostgreSQL con Docker Compose.