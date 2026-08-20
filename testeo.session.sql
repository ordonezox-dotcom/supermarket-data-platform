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
WHERE cliente_id = 2;