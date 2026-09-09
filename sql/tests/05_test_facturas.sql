BEGIN;


-- ============================================================
-- TEST INCREMENTAL - FACTURAS Y DETALLES
--
-- IMPORTANTE:
--
-- Ejecutar este archivo SOLO DESPUÉS de haber ejecutado:
--
-- 01_test_clientes.sql
-- 02_test_productos.sql
-- 03_test_sucursales.sql
-- 04_test_vendedores.sql
--
-- Y posteriormente haber ejecutado:
--
-- Bronze -> Silver -> Gold
--
-- Esto garantiza que los cambios SCD2 de sucursales y
-- vendedores ya tengan su detected_at / fecha_inicio antes
-- de generar estas ventas.
--
-- Casos:
--
-- TEST-INC-001
--   Cliente nuevo
--   Producto nuevo
--   Sucursal nueva
--   Vendedor nuevo
--
-- TEST-INC-002
--   Cliente con nueva versión SCD2
--   Vendedor con nueva versión SCD2
--   Productos con nuevas versiones SCD2
--
-- TEST-INC-003
--   Cliente anónimo
--   Producto con cambio SCD1
--   Vendedor con cambio SCD1
-- ============================================================



-- ============================================================
-- FACTURA 1
--
-- TEST-INC-001
--
-- CLIENTE:
-- Laura Ramirez
-- numero_documento = 9000000051
--
-- SUCURSAL:
-- Supermercado Cali Norte Test
--
-- VENDEDOR:
-- Carlos Mendoza
-- documento = 9000000051
--
-- PRODUCTO:
-- Café Test Incremental 51
-- codigo_barras = 7790000000051
--
-- 2 unidades × 18.500,00
--
-- impuesto_unitario:
-- 18.500 × 19% = 3.515,00
--
-- subtotal = 37.000,00
-- impuesto = 7.030,00
-- total    = 44.030,00
-- ============================================================

WITH factura_1 AS (

    INSERT INTO facturas (
        numero_factura,
        cliente_id,
        sucursal_id,
        vendedor_id,
        fecha_hora,
        metodo_de_pago,
        subtotal,
        descuento_total,
        impuesto_total,
        total,
        estado,
        created_at
    )

    SELECT
        'TEST-INC-001',

        (
            SELECT cliente_id
            FROM clientes
            WHERE numero_documento = '9000000051'
        ),

        (
            SELECT sucursal_id
            FROM sucursales
            WHERE nombre = 'Supermercado Cali Norte Test'
        ),

        (
            SELECT vendedor_id
            FROM vendedores
            WHERE documento = '9000000051'
        ),

        CURRENT_TIMESTAMP,
        'EFECTIVO',

        37000.00,
        0.00,
        7030.00,
        44030.00,

        'PAGADA',
        CURRENT_TIMESTAMP

    RETURNING factura_id
)

INSERT INTO detalles_factura (
    factura_id,
    producto_id,
    cantidad,
    precio_unitario,
    descuento_unitario,
    impuesto_unitario,
    total_linea
)

SELECT
    factura_id,

    (
        SELECT producto_id
        FROM productos
        WHERE codigo_barras = '7790000000051'
    ),

    2,
    18500.00,
    0.00,
    3515.00,
    44030.00

FROM factura_1;



-- ============================================================
-- FACTURA 2
--
-- TEST-INC-002
--
-- Esta es especialmente importante porque utiliza las
-- NUEVAS versiones SCD2.
--
-- CLIENTE:
-- cliente_id = 50
-- ahora ciudad = Cali
--
-- VENDEDOR:
-- vendedor_id = 49
-- ahora sucursal_id = nueva sucursal
--
-- SUCURSAL:
-- Supermercado Cali Norte Test
--
-- PRODUCTOS:
--
-- 46 -> precio_venta cambió
-- 47 -> costo_unitario cambió
-- 48 -> marca cambió
--
-- ============================================================
--
-- PRODUCTO 46
--
-- cantidad = 2
-- precio = 13.499,90
-- impuesto_unitario = 2.564,98
--
-- subtotal = 26.999,80
-- impuesto = 5.129,96
-- total = 32.129,76
--
--
-- PRODUCTO 47
--
-- cantidad = 1
-- precio = 50.008,25
-- impuesto_unitario = 9.501,57
-- total = 59.509,82
--
--
-- PRODUCTO 48
--
-- cantidad = 1
-- precio = 45.111,86
-- impuesto_unitario = 8.571,25
-- total = 53.683,11
--
--
-- TOTAL FACTURA:
--
-- subtotal = 122.119,91
-- impuesto = 23.202,78
-- total = 145.322,69
-- ============================================================

WITH factura_2 AS (

    INSERT INTO facturas (
        numero_factura,
        cliente_id,
        sucursal_id,
        vendedor_id,
        fecha_hora,
        metodo_de_pago,
        subtotal,
        descuento_total,
        impuesto_total,
        total,
        estado,
        created_at
    )

    SELECT
        'TEST-INC-002',

        50,

        (
            SELECT sucursal_id
            FROM sucursales
            WHERE nombre = 'Supermercado Cali Norte Test'
        ),

        49,

        CURRENT_TIMESTAMP,
        'TARJETA_CREDITO',

        122119.91,
        0.00,
        23202.78,
        145322.69,

        'PAGADA',
        CURRENT_TIMESTAMP

    RETURNING factura_id
)

INSERT INTO detalles_factura (
    factura_id,
    producto_id,
    cantidad,
    precio_unitario,
    descuento_unitario,
    impuesto_unitario,
    total_linea
)

SELECT
    factura_id,
    46,
    2,
    13499.90,
    0.00,
    2564.98,
    32129.76
FROM factura_2


UNION ALL


SELECT
    factura_id,
    47,
    1,
    50008.25,
    0.00,
    9501.57,
    59509.82
FROM factura_2


UNION ALL


SELECT
    factura_id,
    48,
    1,
    45111.86,
    0.00,
    8571.25,
    53683.11
FROM factura_2;



-- ============================================================
-- FACTURA 3
--
-- TEST-INC-003
--
-- Venta anónima:
--
-- cliente_id = NULL
--
-- EXPECTATIVA GOLD:
--
-- cliente_sk = 0
--
--
-- VENDEDOR:
-- vendedor 48
--
-- Este vendedor tuvo únicamente cambio SCD1 de correo.
--
-- Continúa perteneciendo a sucursal 10.
--
--
-- PRODUCTO:
-- producto 49
--
-- Tuvo únicamente cambio SCD1:
--
-- Jabones Dove 49
--          ↓
-- Jabón Dove 49
--
-- Precio sigue:
-- 2.050,32
--
-- cantidad = 3
--
-- subtotal:
-- 2.050,32 × 3 = 6.150,96
--
-- impuesto_unitario:
-- 2.050,32 × 19% = 389,56
--
-- impuesto_total:
-- 389,56 × 3 = 1.168,68
--
-- total:
-- 6.150,96 + 1.168,68 = 7.319,64
-- ============================================================

WITH factura_3 AS (

    INSERT INTO facturas (
        numero_factura,
        cliente_id,
        sucursal_id,
        vendedor_id,
        fecha_hora,
        metodo_de_pago,
        subtotal,
        descuento_total,
        impuesto_total,
        total,
        estado,
        created_at
    )

    VALUES (
        'TEST-INC-003',
        NULL,
        10,
        48,
        CURRENT_TIMESTAMP,
        'PSE',
        6150.96,
        0.00,
        1168.68,
        7319.64,
        'PAGADA',
        CURRENT_TIMESTAMP
    )

    RETURNING factura_id
)

INSERT INTO detalles_factura (
    factura_id,
    producto_id,
    cantidad,
    precio_unitario,
    descuento_unitario,
    impuesto_unitario,
    total_linea
)

SELECT
    factura_id,
    49,
    3,
    2050.32,
    0.00,
    389.56,
    7319.64

FROM factura_3;


COMMIT;



-- ============================================================
-- VALIDACIÓN OPERACIONAL
-- ============================================================

SELECT
    f.factura_id,
    f.numero_factura,
    f.cliente_id,
    f.sucursal_id,
    f.vendedor_id,
    f.fecha_hora,
    f.metodo_de_pago,
    f.subtotal,
    f.descuento_total,
    f.impuesto_total,
    f.total,
    f.estado,
    f.created_at
FROM facturas f
WHERE f.numero_factura IN (
    'TEST-INC-001',
    'TEST-INC-002',
    'TEST-INC-003'
)
ORDER BY f.numero_factura;



SELECT
    f.numero_factura,
    d.detalle_id,
    d.producto_id,
    d.cantidad,
    d.precio_unitario,
    d.descuento_unitario,
    d.impuesto_unitario,
    d.total_linea
FROM detalles_factura d

JOIN facturas f
    ON f.factura_id = d.factura_id

WHERE f.numero_factura IN (
    'TEST-INC-001',
    'TEST-INC-002',
    'TEST-INC-003'
)

ORDER BY
    f.numero_factura,
    d.detalle_id;