SET datestyle to DMY;

--Primera tabla datos crudos del csv
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

CREATE TABLE con_fecha_dev(
  periodo TEXT,
  usuario INTEGER,
  fecha_hora_ret TIMESTAMP NOT NULL,
  est_origen INTEGER NOT NULL,
  est_destino INTEGER NOT NULL,
  fecha_hora_dev TIMESTAMP NOT NULL CHECK(fecha_hora_dev >= fecha_hora_ret)
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

CREATE OR REPLACE FUNCTION obtiene_segundo(usr INTEGER, fch TIMESTAMP)
RETURNS VOID
AS $$
DECLARE
  segundo sin_null;
  temp sin_null;
  myCursor1 CURSOR FOR
  SELECT * FROM sin_null
  WHERE usuario = usr and fecha_hora_ret = fch
  ORDER BY tiempo_uso ASC;

BEGIN

  OPEN myCursor1;

  FETCH myCursor1 INTO temp;
  FETCH myCursor1 INTO segundo;

  CLOSE myCursor1;

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
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migracion()
RETURNS void
AS $$

BEGIN
  DELETE FROM bici WHERE usuario IS NULL
                         OR fecha_hora_ret IS NULL
                         OR est_origen IS NULL
                         OR est_destino IS NULL
                         OR tiempo_uso IS NULL
                         OR tiempo_uso LIKE '%-%';
  UPDATE bici
  SET tiempo_uso = REPLACE(REPLACE(REPLACE(tiempo_uso, 'SEG', 's'), 'MIN', 'm'), 'H', 'h');
  INSERT INTO sin_null(
    SELECT periodo, usuario, fecha_hora_ret, est_origen, 
    est_destino, CAST(tiempo_uso AS INTERVAL)
    FROM bici
  );

  INSERT INTO sin_repeticiones_pk(
    SELECT *
    FROM sin_null s1
    WHERE (s1.usuario, s1.fecha_hora_ret) IN (
      SELECT s2.usuario, s2.fecha_hora_ret
      FROM sin_null s2
      GROUP BY s2.usuario, s2.fecha_hora_ret
      HAVING count(s2.usuario) = 1)
    ORDER BY s1.tiempo_uso ASC
  );

  PERFORM borra_pk_repetidas();

  INSERT INTO con_fecha_dev(
    SELECT periodo, usuario, fecha_hora_ret, est_origen, est_destino,
    fecha_hora_ret + tiempo_uso
    FROM sin_repeticiones_pk
    ORDER BY fecha_hora_ret ASC
  );

  INSERT INTO recorrido_final(
    SELECT *
    FROM con_fecha_dev
    WHERE usuario IN (
      SELECT usuario
      FROM con_fecha_dev
      GROUP BY usuario
      HAVING count(usuario) = 1
    )
  );

  PERFORM itera_por_ids();

  --PERFORM limpia_temp();
  --hacer un cursor para seleccionar las cosas con id igual e insertar solo el segundo
  --hacer un cursor para chequear los solapamientos
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION limpia_temp()
RETURNS VOID
AS $$

BEGIN
  DROP TABLE bici;
  DROP TABLE sin_null;
  DROP TABLE sin_repeticiones_pk;
  DROP TABLE con_fecha_dev;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION filtra_solapados(usr con_fecha_dev.usuario%TYPE)
RETURNS VOID
AS $$
DECLARE
  t1 con_fecha_dev;
  periodo TEXT;
  fch_i TIMESTAMP;
  est_i INTEGER;
  est_f INTEGER;
  fch_f TIMESTAMP;

  myCursorOverlaps CURSOR FOR
  SELECT *
  FROM con_fecha_dev
  WHERE usuario = usr
  ORDER BY fecha_hora_ret ASC;

BEGIN

    OPEN myCursorOverlaps;
    FETCH myCursorOverlaps INTO t1;

    LOOP

      EXIT WHEN t1 ISNULL;

      fch_i = t1.fecha_hora_ret;
      fch_f = t1.fecha_hora_dev;
      periodo = t1.periodo;
      est_i = t1.est_origen;
      est_f = t1.est_destino;

      LOOP

        FETCH myCursorOverlaps INTO t1;
        EXIT WHEN NOT FOUND OR t1.fecha_hora_ret > fch_f;

        fch_f = t1.fecha_hora_dev;
        est_f = t1.est_destino;

      END LOOP;

      INSERT INTO recorrido_final VALUES(periodo, usr, fch_i, est_i, est_f, fch_f);

    END LOOP;

    CLOSE myCursorOverlaps;
END;
$$ LANGUAGE plpgSQL;

CREATE OR REPLACE FUNCTION itera_por_ids()
RETURNS VOID
AS $$
DECLARE
    usr con_fecha_dev.usuario%TYPE;
    usuarioCursor CURSOR FOR
    SELECT DISTINCT usuario
    FROM con_fecha_dev;

BEGIN
    OPEN usuarioCursor;
    LOOP

      FETCH usuarioCursor INTO usr;
      EXIT WHEN NOT FOUND;

      PERFORM filtra_solapados(usr);

    END LOOP;
    CLOSE usuarioCursor;
END;
$$ LANGUAGE plpgSQL;

SELECT migracion();

SELECT * FROM con_fecha_dev;
