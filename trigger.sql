CREATE TRIGGER insercion BEFORE INSERT
ON recorrido_final FOR STATEMENT
EXECUTE PROCEDURE trigger_func();
-- La idea de este trigger es chequear para futuras inserciones que se cumplan las restricciones
CREATE OR REPLACE FUNCTION trigger_func()
RETURNS TRIGGER
AS $$
  BEGIN
    --Codigo del trigger
  END;
$$

