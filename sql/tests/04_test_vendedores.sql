BEGIN;


-- ============================================================
-- TEST INCREMENTAL - VENDEDORES
--
-- Casos probados:
--
-- vendedor 48 -> cambio SCD1: correo
-- vendedor 49 -> cambio SCD2: sucursal_id
-- vendedor nuevo -> INSERT
--
-- SCD1:
-- documento
-- nombre
-- apellidos
-- correo
-- fecha_de_contratacion
-- activo
--
-- SCD2:
-- sucursal_id
--
-- IMPORTANTE:
-- vendedores no tiene updated_at.
-- Bronze realiza full reload y Silver usa snapshot_compare.
-- Silver genera detected_at cuando identifica cambios.
-- ============================================================


-- ============================================================
-- 1. CAMBIO SCD1 - CORREO
--
-- vendedor_id = 48
--
-- Yaneth Angarita
--
-- ANTES:
-- correo = yaneth.angarita6202@supermarket.com
--
-- DESPUÉS:
-- correo = yaneth.angarita.test48@supermarket.com
--
-- EXPECTATIVA EN GOLD:
--
-- - NO crear una nueva vendedor_sk.
-- - NO cerrar la versión actual.
-- - Actualizar el correo mediante comportamiento SCD1.
-- ============================================================

UPDATE vendedores
SET
    correo = 'yaneth.angarita.test48@supermarket.com'
WHERE vendedor_id = 48;


-- ============================================================
-- 2. CAMBIO SCD2 - SUCURSAL
--
-- vendedor_id = 49
--
-- Alfonso Quiroga
--
-- ANTES:
-- sucursal_id = 10
--
-- DESPUÉS:
-- sucursal_id = 11
--
-- La sucursal 11 fue creada previamente en
-- 03_test_sucursales.sql:
--
-- Supermercado Cali Norte Test
--
-- Caso de negocio:
-- transferencia del vendedor desde una sucursal existente
-- hacia una nueva sucursal.
--
-- EXPECTATIVA EN GOLD:
--
-- versión anterior:
--   sucursal_id = 10
--   es_actual = false
--   fecha_fin = detected_at
--
-- nueva versión:
--   nueva vendedor_sk
--   sucursal_id = 11
--   fecha_inicio = detected_at
--   fecha_fin = NULL
--   es_actual = true
-- ============================================================

UPDATE vendedores
SET
    sucursal_id = 11
WHERE vendedor_id = 49;


-- ============================================================
-- 3. VENDEDOR NUEVO
--
-- No insertamos vendedor_id manualmente.
-- PostgreSQL debe asignarlo mediante IDENTITY.
--
-- Actualmente el último vendedor conocido es 50,
-- por lo que normalmente esperamos vendedor_id = 51.
--
-- Lo asignamos a la nueva sucursal 11.
--
-- La fecha de contratación debe ser anterior o igual a
-- cualquier factura que posteriormente le asignemos.
--
-- Usamos 2026-09-01 porque:
-- - la sucursal nueva abrió el 2026-09-01
-- - el vendedor puede haberse contratado ese mismo día
-- - las facturas de prueba las crearemos después de esa fecha
-- ============================================================

INSERT INTO vendedores (
    sucursal_id,
    documento,
    nombre,
    apellidos,
    correo,
    fecha_de_contratacion,
    activo,
    create_at
)
VALUES (
    11,
    '9000000051',
    'Carlos',
    'Mendoza',
    'carlos.mendoza.test51@supermarket.com',
    DATE '2026-09-01',
    TRUE,
    CURRENT_TIMESTAMP
);


COMMIT;


-- ============================================================
-- VALIDACIÓN OPERACIONAL
-- ============================================================

SELECT
    vendedor_id,
    sucursal_id,
    documento,
    nombre,
    apellidos,
    correo,
    fecha_de_contratacion,
    activo,
    create_at
FROM vendedores
WHERE
    vendedor_id IN (48, 49)
    OR documento = '9000000051'
ORDER BY vendedor_id;