package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	pgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func RunMigrations(ctx context.Context, databaseURL, migrationsPath string) error {
	absMigrationsPath, err := filepath.Abs(migrationsPath)
	if err != nil {
		return fmt.Errorf("resolve migrations path: %w", err)
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open migration database: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping migration database: %w", err)
	}

	driver, err := pgx.WithInstance(db, &pgx.Config{})
	if err != nil {
		return fmt.Errorf("create migration driver: %w", err)
	}

	sourcePath := filepath.ToSlash(absMigrationsPath)
	if !strings.HasPrefix(sourcePath, "/") {
		sourcePath = "/" + sourcePath
	}
	sourceURL := (&url.URL{Scheme: "file", Path: sourcePath}).String()
	migrator, err := migrate.NewWithDatabaseInstance(sourceURL, "pgx5", driver)
	if err != nil {
		return fmt.Errorf("initialize migrations: %w", err)
	}
	defer migrator.Close()

	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}

	return nil
}
