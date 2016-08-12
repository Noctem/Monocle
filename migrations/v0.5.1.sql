# MySQL only; SQLite doesn't support CHANGE COLUMN. You need to export data,
# remove database, recreate database and import data. Be careful while doing
# it, or you may lose what you gathered.
ALTER TABLE `sightings` CHANGE COLUMN `lat` `lat` VARCHAR(20);
ALTER TABLE `sightings` CHANGE COLUMN `lon` `lon` VARCHAR(20);
