package goose

import (
	"database/sql"
)

// UpTo migrates up to a specific version.
func UpTo(db *sql.DB, dir string, version int64) error {
	migrations, err := CollectMigrations(dir, minVersion, version)
	if err != nil {
		return err
	}

	// ensure the db table exists
	if _, err := EnsureDBVersion(db); err != nil {
		return err
	}

	records, err := GetDBRecords(db)
	if err != nil {
		return err
	}

	OUTER:
	for _, migration := range migrations {
		// skip existing migrations
		for _, record := range records {
			if record.VersionID == migration.Version {
				continue OUTER
			}
		}
		if err = migration.Up(db); err != nil {
			return err
		}
	}

	current, err := GetDBVersion(db)
	if err != nil {
		return err
	}
	log.Printf("goose: no more migrations to run. current version: %d\n", current)
	return nil
}

// Up applies all available migrations.
func Up(db *sql.DB, dir string) error {
	return UpTo(db, dir, maxVersion)
}

// UpByOne migrates up by a single version.
func UpByOne(db *sql.DB, dir string) error {
	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	currentVersion, err := GetDBVersion(db)
	if err != nil {
		return err
	}

	next, err := migrations.Next(currentVersion)
	if err != nil {
		if err == ErrNoNextVersion {
			log.Printf("goose: no migrations to run. current version: %d\n", currentVersion)
		}
		return err
	}

	if err = next.Up(db); err != nil {
		return err
	}

	return nil
}
