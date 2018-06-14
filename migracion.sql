DROP TABLE  bici;
--Entrada del CSV Correr desde PSQL por terminal el siguiente comando: "\copy bici FROM test1.csv csv header delimiter ';'".
-- Antes tienen que haber copiado el archivo a su cuenta de pampero

--Primera tabla datos crudos del csv

SET datestyle to DMY;

CREATE TABLE bici (
  periodo TEXT,
  idUsuario INTEGER,
  fechaHoraRetiro TIMESTAMP,
  origenEstacion INTEGER NOT NULL,
  nombreOrigen TEXT,
  detinoEstacion INTEGER NOT NULL,
  nombreDestino TEXT,
  tiempoUso TEXT,
  fechaCreacion TIMESTAMP
);

-- Tabla final
CREATE TABLE recorrido_final (
  periodo TEXT,
  usuario INTEGER,
  fecha_hora_ret TIMESTAMP NOT NULL,
  est_origen INTEGER NOT NULL,
  est_destino INTEGER NOT NULL,
  fecha_hora_dev TIMESTAMP NOT NULL CHECK(fecha_hora_dev >= fecha_hora_ret)
,
PRIMARY KEY(usuario,fecha_hora_ret));


CREATE OR REPLACE FUNCTION migracion()
RETURNS void
AS $$

BEGIN
  DELETE FROM bici WHERE idUsuario IS NULL
                         OR fechaHoraRetiro IS NULL
                         OR origenEstacion IS NULL
                         OR detinoEstacion IS NULL
                         OR tiempoUso IS NULL;
  --hacer un cursor para seleccionar las cosas con id igual e insertar solo el segundo
  --hacer un cursor para chequear los solapamientos
END
$$ LANGUAGE plpgSQL;

SELECT migracion();

SELECT * FROM bici;

