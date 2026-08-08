SELECT
    f.numero_factura,
    p.nombre AS producto,
    d.cantidad,
    d.precio_unitario,
    d.descuento_unitario,
    d.impuesto_unitario,
    d.total_linea
FROM detalles_factura AS d
INNER JOIN facturas AS f
    ON d.factura_id = f.factura_id
INNER JOIN productos AS p
    ON d.producto_id = p.producto_id
ORDER BY d.detalle_id
LIMIT 30;