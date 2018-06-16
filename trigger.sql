
-- La funcion de este trigger es chequear para futuras inserciones que se cumplan las restricciones
CREATE OR REPLACE FUNCTION trigger_func()
RETURNS TRIGGER
AS $$
DECLARE
  sobreps INTEGER;
  BEGIN
        sobreps = (SELECT COUNT(*)
        FROM recorrido_final
        WHERE NEW.usuario = usuario AND (((NEW.fecha_hora_dev = fecha_hora_ret) OR (fecha_hora_dev = NEW.fecha_hora_ret))
                                         OR ((NEW.fecha_hora_dev > fecha_hora_ret AND NEW.fecha_hora_ret < fecha_hora_ret)
                                             OR (fecha_hora_dev > NEW.fecha_hora_ret AND fecha_hora_ret < NEW.fecha_hora_ret))
                                         OR (NEW.fecha_hora_ret = fecha_hora_ret)
                                         OR ((NEW.fecha_hora_ret > fecha_hora_ret AND NEW.fecha_hora_dev < fecha_hora_dev)
                                             OR (fecha_hora_ret>NEW.fecha_hora_ret AND fecha_hora_dev < NEW.fecha_hora_dev))
                                         OR (fecha_hora_dev = NEW.fecha_hora_dev)
                                         OR ((NEW.fecha_hora_ret = fecha_hora_ret) AND (NEW.fecha_hora_dev = fecha_hora_dev))));

        IF sobreps > 0 THEN RAISE EXCEPTION 'Se ha producido un error en la inserción: ' ||
                                            'El intervalo se solapa'; END IF;
				RETURN NEW;

  END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insercion BEFORE INSERT ON recorrido_final FOR EACH ROW EXECUTE PROCEDURE trigger_func();