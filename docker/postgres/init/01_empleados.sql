-- Dataset de ejemplo para el ejemplo de Spark que lee vía JDBC
-- (01_connect_postgres.py en la raíz del proyecto) y para consultar desde
-- Hue. Solo se ejecuta la primera vez que arranca el contenedor (con el
-- volumen de datos vacío) — la imagen oficial de postgres ejecuta todo lo
-- que hay en /docker-entrypoint-initdb.d/ en ese momento, nunca después.
CREATE TABLE empleados (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    departamento VARCHAR(50) NOT NULL,
    salario NUMERIC(10, 2) NOT NULL,
    fecha_alta DATE NOT NULL
);

INSERT INTO empleados (nombre, departamento, salario, fecha_alta) VALUES
    ('Ana García', 'Ingeniería', 42000.00, '2021-03-15'),
    ('Bruno Ruiz', 'Ingeniería', 45500.00, '2020-07-01'),
    ('Carla Díaz', 'Ventas', 38000.00, '2022-01-10'),
    ('David López', 'Ventas', 39500.00, '2019-11-20'),
    ('Elena Torres', 'Marketing', 41000.00, '2023-02-05'),
    ('Fran Molina', 'Ingeniería', 51000.00, '2018-05-30'),
    ('Gema Ortiz', 'Marketing', 37500.00, '2021-09-12'),
    ('Hugo Navarro', 'Ventas', 40200.00, '2020-12-01');
