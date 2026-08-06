SELECT
    v.vendedor_id,
    v.nombre,
    v.apellidos,
    v.correo,
    v.fecha_de_contratacion,
    s.nombre AS sucursal,
    s.ciudad
FROM vendedores AS v
INNER JOIN sucursales AS s
    ON v.sucursal_id = s.sucursal_id
ORDER BY
    s.sucursal_id,
    v.vendedor_id;