# convert existing spawn_id hex strings to integers
# set SPAWN_ID_INT to True after running
# only works on PostgreSQL

# create function for hex conversion
CREATE OR REPLACE FUNCTION hex_to_bigint(hexval varchar) RETURNS bigint AS $$
DECLARE
    result  bigint;
BEGIN
    EXECUTE 'SELECT x''' || hexval || '''::bigint' INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

# increase size of columns to temporarily fit expanded values
# not necessary if your column is already text or >16 characters
ALTER TABLE sightings
ALTER COLUMN spawn_id TYPE character varying (17);

ALTER TABLE longspawns
ALTER COLUMN spawn_id TYPE character varying (17);

# convert hex strings to bigints, may take a while on large databases
UPDATE sightings SET spawn_id = hex_to_bigint(spawn_id);
UPDATE longspawns SET spawn_id = hex_to_bigint(spawn_id);

# convert column data types to bigints
ALTER TABLE sightings
ALTER COLUMN spawn_id TYPE bigint USING spawn_id::bigint;
ALTER TABLE longspawns
ALTER COLUMN spawn_id TYPE bigint USING spawn_id::bigint;
