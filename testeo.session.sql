SELECT
    detalle_id,
    factura_id,
    producto_id,
    cantidad,
    precio_unitario,
    descuento_unitario,
    impuesto_unitario,
    total_linea
FROM detalles_factura
WHERE factura_id IN (49, 50)
ORDER BY factura_id, detalle_id;