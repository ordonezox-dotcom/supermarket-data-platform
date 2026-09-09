BEGIN;


-- ============================================================
-- TEST INCREMENTAL - CLIENTES
--
-- Casos probados:
--
-- 1. Cliente 49 -> cambio SCD1
-- 2. Cliente 50 -> cambio SCD2
-- 3. Cliente nuevo -> INSERT
--
-- SCD1:
-- tipo_documento
-- numero_documento
-- nombre
-- apellido
-- correo
-- telefono
-- fecha_nacimiento
-- activo
--
-- SCD2:
-- ciudad
-- ============================================================


-- ============================================================
-- 1. CAMBIO SCD1
--
-- Cliente:
-- cliente_id = 49
-- María Ortiz
--
-- Valor anterior:
-- telefono = 3268117758
--
-- Valor nuevo:
-- telefono = 3159000049
--
-- EXPECTATIVA EN GOLD:
--
-- - NO crear una nueva cliente_sk.
-- - NO crear una nueva versión SCD2.
-- - Actualizar el teléfono mediante comportamiento SCD1.
-- ============================================================

UPDATE clientes
SET
    telefono = '3159000049',
    updated_at = CURRENT_TIMESTAMP
WHERE cliente_id = 49;


-- ============================================================
-- 2. CAMBIO SCD2
--
-- Cliente:
-- cliente_id = 50
-- Maira Torres
--
-- Valor anterior:
-- ciudad = Bucaramanga
--
-- Valor nuevo:
-- ciudad = Cali
--
-- EXPECTATIVA EN GOLD:
--
-- - Cerrar la versión actual.
-- - fecha_fin = nuevo updated_at.
-- - es_actual = false en la versión anterior.
-- - Crear una nueva cliente_sk.
-- - Nueva versión:
--       ciudad = Cali
--       fecha_inicio = updated_at
--       fecha_fin = NULL
--       es_actual = true
-- ============================================================

UPDATE clientes
SET
    ciudad = 'Cali',
    updated_at = CURRENT_TIMESTAMP
WHERE cliente_id = 50;


-- ============================================================
-- 3. CLIENTE NUEVO
--
-- No insertamos cliente_id manualmente.
-- PostgreSQL debe asignarlo mediante IDENTITY.
--
-- Como actualmente el último cliente es 50,
-- esperamos normalmente cliente_id = 51.
--
-- fecha_registro representa cuándo realmente se convirtió
-- en cliente.
--
-- Como estamos simulando un cliente nuevo que acaba de
-- registrarse, usamos CURRENT_TIMESTAMP.
-- ============================================================

INSERT INTO clientes (
    tipo_documento,
    numero_documento,
    nombre,
    apellido,
    correo,
    telefono,
    ciudad,
    fecha_nacimiento,
    fecha_registro,
    activo,
    updated_at
)
VALUES (
    'CC',
    '9000000051',
    'Laura',
    'Ramirez',
    'laura.ramirez.test51@example.com',
    '3159000051',
    'Cali',
    DATE '1995-05-20',
    CURRENT_TIMESTAMP,
    TRUE,
    CURRENT_TIMESTAMP
);


COMMIT;


-- ============================================================
-- VALIDACIÓN OPERACIONAL
--
-- Mostramos los tres registros utilizados en esta prueba.
-- ============================================================

SELECT
    cliente_id,
    tipo_documento,
    numero_documento,
    nombre,
    apellido,
    correo,
    telefono,
    ciudad,
    fecha_nacimiento,
    fecha_registro,
    activo,
    updated_at
FROM clientes
WHERE
    cliente_id IN (49, 50)
    OR numero_documento = '9000000051'
ORDER BY cliente_id;