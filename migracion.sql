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
  tiempoUso TEXT, --convertir de str a interval reemplazando h,m,s por : :
  fechaCreacion TIMESTAMP
);

\copy bici from test1.csv header delimiter ';' csv;

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

/*probablemente no termine siendo create table pero la logica sirve para
seleccionar tuplas q tengan pks repetidas*/
CREATE TABLE pkRepeated AS (
  SELECT *
  FROM bici b1
  WHERE b1.id_usuario IN (
    SELECT id
    FROM bici b2
    GROUP BY b2.usuario, b2.fechaHoraRetiro
    HAVING count(usuario) > 1)
  ORDER BY b1.usuario ASC);

/*probablemente no se use mas tarde*/
DELETE FROM bici WHERE EXISTS(
    SELECT * FROM pkRepeated
    WHERE bici.usuario == pkRepeated.usuario
      AND bici.fechaHoraRetiro == pkRepeated.fechaHoraRetiro);

CREATE OR REPLACE FUNCTION migracion()
RETURNS void
AS $$

BEGIN
  DELETE FROM bici WHERE idUsuario IS NULL
                         OR fechaHoraRetiro IS NULL
                         OR origenEstacion IS NULL
                         OR detinoEstacion IS NULL
                         OR tiempoUso IS NULL;
  UPDATE bici
  SET tiempoUso = REPLACE(REPLACE(REPLACE(tiempoUso, 'SEG', 's'), 'MIN', 'm'), 'H', 'h');
  --mandar a tabla final

  --hacer un cursor para seleccionar las cosas con id igual e insertar solo el segundo
  --hacer un cursor para chequear los solapamientos
END
$$ LANGUAGE plpgSQL;

SELECT migracion();

SELECT * FROM bici;
