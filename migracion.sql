DROP TABLE  bici;
-- Antes tienen que haber copiado el archivo a su cuenta de pampero

--Primera tabla datos crudos del csv

SET datestyle to DMY;

-- tabla que se llenara con datos del csv al comienzo
CREATE TABLE bici (
  periodo TEXT,
  usuario INTEGER,
  fecha_hora_ret TIMESTAMP,
  est_origen INTEGER NOT NULL,
  nombre_origen TEXT,
  est_destino INTEGER NOT NULL,
  nombre_destino TEXT,
  tiempo_uso TEXT,
  fecha_creacion TIMESTAMP
);

\copy bici from test1.csv header delimiter ';' csv;

-- tabla temp que filtra los null y cambia tiempo_uso a interval
CREATE TABLE sin_null (
  periodo TEXT,
  usuario INTEGER,
  fecha_hora_ret TIMESTAMP NOT NULL,
  est_origen INTEGER NOT NULL,
  est_destino INTEGER NOT NULL,
  tiempo_uso INTERVAL NOT NULL
  --fecha_hora_dev TIMESTAMP NOT NULL CHECK(fecha_hora_dev >= fecha_hora_ret)
);

-- tabla temp donde se filtraran las repeticiones de pk segun pedido
SELECT * INTO sin_repeticiones_pk FROM sin_null;

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

CREATE OR REPLACE FUNCTION obtiene_segundo(usr INTEGER, fch TIMESTAMP)
RETURNS VOID
AS $$
DECLARE
  segundo sin_null;
  temp sin_null;
  myCursor CURSOR FOR
  SELECT * FROM sin_null
  WHERE usuario = usr and fecha_hora_ret = fch
  ORDER BY tiempo_uso ASC;

BEGIN

  OPEN myCursor;

  FETCH myCursor INTO temp;
  FETCH myCursor INTO segundo;

  CLOSE myCursor;

  INSERT INTO sin_repeticiones_pk
    VALUES(segundo.periodo, segundo.usuario, segundo.fecha_hora_ret,
    segundo.est_origen, segundo.est_destino, segundo.tiempo_uso);
END;
$$ LANGUAGE plpgSQL;

/*
 * Elige todas las pk distintas que aparecen mas de una vez, para luego
 * llamar a una funcion que, en base a esas pk, hace una consulta de
 * las tuplas donde aparece esa pk, las ordena por tiempo_uso y agarra la 2da.
 */
CREATE OR REPLACE FUNCTION borra_pk_repetidas()
RETURNS VOID
AS $$
DECLARE
    usr sin_null.usuario%TYPE;
    fch sin_null.fecha_hora_ret%TYPE;
    myCursor CURSOR FOR
    SELECT DISTINCT usuario, fecha_hora_ret
    FROM sin_null
    GROUP BY usuario, fecha_hora_ret
    HAVING count(usuario) > 1;

BEGIN
    OPEN myCursor;
    LOOP

      FETCH myCursor INTO usr, fch;
      EXIT WHEN NOT FOUND;

      PERFORM obtiene_segundo(usr, fch);

    END LOOP;
    CLOSE myCursor;

    RAISE NOTICE '%', usr;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migracion()
RETURNS void
AS $$

BEGIN
  DELETE FROM bici WHERE usuario IS NULL
                         OR fecha_hora_retiro IS NULL
                         OR est_origen IS NULL
                         OR est_destino IS NULL
                         OR tiempo_uso IS NULL
                         OR tiempo_uso LIKE '%-%';
  UPDATE bici
  SET tiempo_uso = REPLACE(REPLACE(REPLACE(tiempo_uso, 'SEG', 's'), 'MIN', 'm'), 'H', 'h');
  INSERT INTO sin_null(
    SELECT periodo, usuario, fecha_hora_ret, est_origen, est_destino, tiempo_uso
  );

  INSERT INTO sin_repeticiones_pk(
    SELECT *
    FROM sin_null s1
    WHERE (s1.usuario, s1.fecha_hora_retiro) IN (
      SELECT s2.usuario, s2.fecha_hora_retiro
      FROM sin_null s2
      GROUP BY s2.usuario, s2.fecha_hora_retiro
      HAVING count(s2.usuario) = 1)
    ORDER BY s1.tiempo_uso ASC
  );

  PERFORM borra_pk_repetidas();
  --hacer un cursor para seleccionar las cosas con id igual e insertar solo el segundo
  --hacer un cursor para chequear los solapamientos
END
$$ LANGUAGE plpgsql;

SELECT migracion();

SELECT * FROM bici;
