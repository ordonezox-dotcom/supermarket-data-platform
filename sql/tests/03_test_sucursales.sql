BEGIN;


-- ============================================================
-- TEST INCREMENTAL - SUCURSALES
--
-- Casos probados:
--
-- sucursal 7 -> cambio SCD1: nombre
-- sucursal 8 -> cambio SCD2: ciudad
-- sucursal 9 -> cambio SCD2: direccion
-- sucursal nueva -> INSERT
--
-- SCD1:
-- nombre
-- fecha_apertura
-- activa
--
-- SCD2:
-- ciudad
-- direccion
--
-- IMPORTANTE:
-- La tabla operacional sucursales no tiene updated_at.
-- Bronze realiza full reload y Silver usa snapshot_compare.
-- Por eso los cambios se detectarán mediante comparación
-- entre snapshots y Silver generará detected_at.
-- ============================================================


-- ============================================================
-- 1. CAMBIO SCD1 - NOMBRE
--
-- sucursal_id = 7
--
-- ANTES:
-- nombre = Supermercado El Poblado
--
-- DESPUÉS:
-- nombre = Supermercado El Poblado Premium
--
-- EXPECTATIVA EN GOLD:
--
-- - NO crear nueva sucursal_sk.
-- - NO generar una nueva versión SCD2.
-- - Actualizar nombre mediante comportamiento SCD1.
-- ============================================================

UPDATE sucursales
SET
    nombre = 'Supermercado El Poblado Premium'
WHERE sucursal_id = 7;


-- ============================================================
-- 2. CAMBIO SCD2 - CIUDAD
--
-- sucursal_id = 8
--
-- ANTES:
-- ciudad = Medellín
--
-- DESPUÉS:
-- ciudad = Envigado
--
-- Caso de negocio:
-- simulamos una reclasificación territorial de la sucursal.
--
-- EXPECTATIVA EN GOLD:
--
-- - cerrar versión anterior
-- - es_actual = false
-- - fecha_fin = detected_at
-- - crear nueva sucursal_sk
-- - nueva versión con ciudad = Envigado
-- ============================================================

UPDATE sucursales
SET
    ciudad = 'Envigado'
WHERE sucursal_id = 8;


-- ============================================================
-- 3. CAMBIO SCD2 - DIRECCION
--
-- sucursal_id = 9
--
-- ANTES:
-- direccion = Carrera 53 # 98-50
--
-- DESPUÉS:
-- direccion = Carrera 53 # 98-80
--
-- Caso de negocio:
-- actualización realista de ubicación/dirección registrada.
--
-- EXPECTATIVA EN GOLD:
--
-- - cerrar versión anterior
-- - crear nueva versión SCD2
-- - nueva sucursal_sk
-- ============================================================

UPDATE sucursales
SET
    direccion = 'Carrera 53 # 98-80'
WHERE sucursal_id = 9;


-- ============================================================
-- 4. SUCURSAL NUEVA
--
-- No insertamos sucursal_id manualmente.
-- PostgreSQL debe asignarlo mediante IDENTITY.
--
-- Actualmente el último ID conocido es 10,
-- así que normalmente esperamos sucursal_id = 11.
--
-- La fecha_apertura es coherente con la fecha actual:
-- simulamos una sucursal nueva que abre en 2026.
-- ============================================================

INSERT INTO sucursales (
    nombre,
    ciudad,
    direccion,
    fecha_apertura,
    activa,
    create_at
)
VALUES (
    'Supermercado Cali Norte Test',
    'Cali',
    'Avenida 6N # 35-20',
    DATE '2026-09-01',
    TRUE,
    CURRENT_TIMESTAMP
);


COMMIT;


-- ============================================================
-- VALIDACIÓN OPERACIONAL
-- ============================================================

SELECT
    sucursal_id,
    nombre,
    ciudad,
    direccion,
    fecha_apertura,
    activa,
    create_at
FROM sucursales
WHERE
    sucursal_id IN (7, 8, 9)
    OR nombre = 'Supermercado Cali Norte Test'
ORDER BY sucursal_id;