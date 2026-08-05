CREATE TABLE sucursales(
    sucursal_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    ciudad VARCHAR(100) NOT NULL,
    direccion VARCHAR(200),
    fecha_apertura DATE,
    activa BOOLEAN NOT NULL DEFAULT TRUE,
    create_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE vendedores(
    vendedor_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sucursal_id INTEGER NOT NULL,
    documento VARCHAR(30) NOT NULL UNIQUE,
    nombre VARCHAR(100) NOT NULL,
    apellidos VARCHAR(100) NOT NULL,
    correo VARCHAR(150),
    fecha_de_contratacion DATE NOT NULL,
    activo BOOLEAN NOT NULL DEFAULT TRUE,
    create_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_vendedor_sucursal
        FOREIGN KEY (sucursal_id)
        REFERENCES sucursales(sucursal_id)

);

CREATE TABLE clientes (
    cliente_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tipo_documento VARCHAR(10),
    numero_documento VARCHAR(30) UNIQUE,
    nombre VARCHAR(100),
    apellido VARCHAR(100),
    correo VARCHAR(150),
    telefono VARCHAR(30),
    ciudad VARCHAR(100),
    fecha_nacimiento DATE,
    fecha_registro TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    activo BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE productos (
    producto_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    codigo_barras VARCHAR(50) NOT NULL UNIQUE,
    nombre VARCHAR(150) NOT NULL,
    categoria VARCHAR(100) NOT NULL,
    subcategoria VARCHAR(100),
    marca VARCHAR(100),
    precio_venta NUMERIC(12, 2) NOT NULL,
    costo_unitario NUMERIC(12, 2) NOT NULL,
    activo BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_precio_venta_positivo
        CHECK (precio_venta >= 0),

    CONSTRAINT chk_costo_unitario_positivo
        CHECK (costo_unitario >= 0)
);

CREATE TABLE facturas (
    factura_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    numero_factura VARCHAR(50) NOT NULL UNIQUE,
    cliente_id INTEGER,
    sucursal_id INTEGER NOT NULL,
    vendedor_id INTEGER NOT NULL,
    fecha_hora TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metodo_de_pago VARCHAR(30) NOT NULL,
    subtotal NUMERIC(14,2) NOT NULL,
    descuento_total NUMERIC(14,2) NOT NULL DEFAULT 0,
    impuesto_total NUMERIC(14,2) NOT NULL DEFAULT 0,
    total NUMERIC(14,2) NOT NULL,
    estado VARCHAR(20) NOT NULL DEFAULT 'PAGADA',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_factura_cliente
        FOREIGN KEY (cliente_id)
        REFERENCES clientes(cliente_id),

    CONSTRAINT fk_factura_sucursal
        FOREIGN KEY (sucursal_id)
        REFERENCES sucursales(sucursal_id),

    CONSTRAINT fk_factura_vendedor
        FOREIGN KEY (vendedor_id)
        REFERENCES vendedores(vendedor_id),

    CONSTRAINT chk_factura_subtotal
        CHECK (subtotal >=0),

    CONSTRAINT chk_factura_descuento
        CHECK (descuento_total >=0),

    CONSTRAINT chk_factura_impuesto
        CHECK (impuesto_total >=0),

    CONSTRAINT chk_factura_total
        CHECK (total >=0),

    CONSTRAINT chk_factura_estado
        CHECK (estado IN ('PAGADA','NULA','DEVUELTA'))

);

CREATE TABLE detalles_factura (
    detalle_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    factura_id INTEGER NOT NULL,
    producto_id INTEGER NOT NULL,
    cantidad INTEGER NOT NULL,
    precio_unitario NUMERIC(12,2) NOT NULL,
    descuento_unitario NUMERIC(12,2) NOT NULL DEFAULT 0,
    impuesto_unitario NUMERIC(12,2) NOT NULL DEFAULT 0,
    total_linea NUMERIC(12,2) NOT NULL,
    created_ad TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_detalle_factura
        FOREIGN KEY (factura_id)
        REFERENCES facturas(factura_id),

    CONSTRAINT fk_detalle_producto
        FOREIGN KEY (producto_id)
        REFERENCES productos(producto_id),

    CONSTRAINT chk_detalle_cantidad
        CHECK(cantidad > 0),

    CONSTRAINT chk_detalle_precio
        CHECK (precio_unitario >= 0),

    CONSTRAINT chk_detalle_descuento
        CHECK (descuento_unitario >= 0),

    CONSTRAINT chk_detalle_impuesto
        CHECK (impuesto_unitario >= 0),
    
    CONSTRAINT chk_detalle_total
        CHECK (total_linea >= 0),

    CONSTRAINT uq_detalle_producto
        UNIQUE (factura_id, producto_id)

);

CREATE TABLE inventario (
    inventario_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sucursal_id INTEGER NOT NULL,
    producto_id INTEGER NOT NULL,
    cantidad_disponible INTEGER NOT NULL DEFAULT 0,
    stock_minimo INTEGER NOT NULL DEFAULT 0,
    stock_maximo INTEGER,
    fecha_actualizacion TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_inventario_sucursal
        FOREIGN KEY (sucursal_id)
        REFERENCES sucursales (sucursal_id),

    CONSTRAINT fk_inventario_producto
        FOREIGN KEY (producto_id)
        REFERENCES productos(producto_id),

    CONSTRAINT chk_inventario_cantidad
        CHECK (cantidad_disponible >= 0),

    CONSTRAINT chk_inventario_minimo
        CHECK (stock_minimo >= 0),

    CONSTRAINT chk_inventario_maximo
        CHECK (
            stock_maximo IS NULL
            OR stock_maximo >= stock_minimo
        ),

    CONSTRAINT uq_inventario_sucursal_producto
        UNIQUE (sucursal_id, producto_id) 

);

CREATE INDEX idx_facturas_fecha_hora
    ON facturas (fecha_hora);

CREATE INDEX idx_factura_cliente
    ON facturas (cliente_id);

CREATE INDEX idx_factura_sucursal
    ON facturas (sucursal_id);

CREATE INDEX idx_detalle_factura_factura
    ON detalles_factura (factura_id);

CREATE INDEX idx_detalle_factura_producto
    ON detalles_factura (producto_id);

CREATE INDEX idx_inventario_sucursal
    ON inventario (sucursal_id);

CREATE INDEX idx_inventario_producto
    ON inventario (producto_id);