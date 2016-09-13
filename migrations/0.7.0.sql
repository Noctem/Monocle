# for use with PostgreSQL and MySQL (untested)
# for SQLite, recreate database and import existing data

ALTER TABLE sightings
ALTER COLUMN encounter_id TYPE numeric(20,0) USING encounter_id::numeric(20,0);

ALTER TABLE longspawns
ALTER COLUMN encounter_id TYPE numeric(20,0) USING encounter_id::numeric(20,0);

ALTER TABLE longspawns
DROP COLUMN normalized_timestamp;

ALTER TABLE fort_sightings
ALTER COLUMN last_modified TYPE integer;

