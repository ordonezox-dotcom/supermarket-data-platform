BEGIN;


-- ============================================================
-- TEST INCREMENTAL - PRODUCTOS
--
-- Casos probados:
--
-- producto 44 -> SCD2: categoria
-- producto 45 -> SCD2: subcategoria
-- producto 46 -> SCD2: precio_venta
-- producto 47 -> SCD2: costo_unitario
-- producto 48 -> SCD2: marca
-- producto 49 -> SCD1: nombre
-- producto nuevo -> INSERT
--
-- SCD1:
-- codigo_barras
-- nombre
-- activo
--
-- SCD2:
-- categoria
-- subcategoria
-- marca
-- precio_venta
-- costo_unitario
-- ============================================================


-- ============================================================
-- 1. SCD2 - CAMBIO DE CATEGORIA
--
-- producto_id = 44
--
-- Producto:
-- Detergentes Ariel 44
--
-- ANTES:
-- categoria = Aseo
-- subcategoria = Detergentes
--
-- DESPUÉS:
-- categoria = Limpieza
--
-- EXPECTATIVA GOLD:
-- cerrar versión anterior y crear una nueva producto_sk.
-- ============================================================

UPDATE productos
SET
    categoria = 'Limpieza',
    updated_at = CURRENT_TIMESTAMP
WHERE producto_id = 44;


-- ============================================================
-- 2. SCD2 - CAMBIO DE SUBCATEGORIA
--
-- producto_id = 45
--
-- Producto:
-- Jabones Palmolive 45
--
-- ANTES:
-- categoria = Aseo
-- subcategoria = Jabones
--
-- DESPUÉS:
-- subcategoria = Higiene Personal
--
-- El producto ya estaba inactivo y seguirá inactivo.
--
-- Esta prueba únicamente comprueba que subcategoria
-- dispara una nueva versión SCD2.
-- ============================================================

UPDATE productos
SET
    subcategoria = 'Higiene Personal',
    updated_at = CURRENT_TIMESTAMP
WHERE producto_id = 45;


-- ============================================================
-- 3. SCD2 - CAMBIO DE PRECIO DE VENTA
--
-- producto_id = 46
--
-- Producto:
-- Leche Alquería 46
--
-- ANTES:
-- precio_venta = 12931.65
--
-- DESPUÉS:
-- precio_venta = 13499.90
--
-- Caso de negocio:
-- actualización normal del precio comercial del producto.
-- ============================================================

UPDATE productos
SET
    precio_venta = 13499.90,
    updated_at = CURRENT_TIMESTAMP
WHERE producto_id = 46;


-- ============================================================
-- 4. SCD2 - CAMBIO DE COSTO UNITARIO
--
-- producto_id = 47
--
-- Producto:
-- Jugos Tutti Frutti 47
--
-- ANTES:
-- costo_unitario = 37602.57
--
-- DESPUÉS:
-- costo_unitario = 38950.00
--
-- Caso de negocio:
-- cambio del costo de adquisición del producto.
-- ============================================================

UPDATE productos
SET
    costo_unitario = 38950.00,
    updated_at = CURRENT_TIMESTAMP
WHERE producto_id = 47;


-- ============================================================
-- 5. SCD2 - CAMBIO DE MARCA
--
-- producto_id = 48
--
-- Producto:
-- Yogurt Alpina 48
--
-- ANTES:
-- marca = Alpina
--
-- DESPUÉS:
-- marca = Alpina Selección
--
-- Elegimos una variante coherente con el nombre comercial
-- existente para no generar una combinación absurda.
-- ============================================================

UPDATE productos
SET
    marca = 'Alpina Selección',
    updated_at = CURRENT_TIMESTAMP
WHERE producto_id = 48;


-- ============================================================
-- 6. SCD1 - CAMBIO DE NOMBRE
--
-- producto_id = 49
--
-- ANTES:
-- nombre = Jabones Dove 49
--
-- DESPUÉS:
-- nombre = Jabón Dove 49
--
-- EXPECTATIVA GOLD:
--
-- - NO crear una nueva producto_sk.
-- - NO generar una nueva versión SCD2.
-- - El nombre debe actualizarse mediante comportamiento SCD1.
-- ============================================================

UPDATE productos
SET
    nombre = 'Jabón Dove 49',
    updated_at = CURRENT_TIMESTAMP
WHERE producto_id = 49;


-- ============================================================
-- 7. PRODUCTO NUEVO
--
-- No especificamos producto_id.
-- PostgreSQL debe utilizar la columna IDENTITY.
--
-- Actualmente el último producto conocido es 50,
-- así que normalmente esperamos producto_id = 51.
--
-- Producto diseñado para poder utilizarlo posteriormente
-- en las facturas de prueba.
-- ============================================================

INSERT INTO productos (
    codigo_barras,
    nombre,
    categoria,
    subcategoria,
    marca,
    precio_venta,
    costo_unitario,
    activo,
    created_at,
    updated_at
)
VALUES (
    '7790000000051',
    'Café Test Incremental 51',
    'Alimentos',
    'Café',
    'Café Test',
    18500.00,
    12500.00,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
);


COMMIT;


-- ============================================================
-- VALIDACIÓN OPERACIONAL
-- ============================================================

SELECT
    producto_id,
    codigo_barras,
    nombre,
    categoria,
    subcategoria,
    marca,
    precio_venta,
    costo_unitario,
    activo,
    created_at,
    updated_at
FROM productos
WHERE
    producto_id BETWEEN 44 AND 49
    OR codigo_barras = '7790000000051'
ORDER BY producto_id;