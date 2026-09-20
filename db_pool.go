package main

import "time"

func OpenEventStore(databaseURL string) (*EventStore, error) {
	maxOpenConns := getEnvAsInt("DB_MAX_OPEN_CONNS", 20)
	maxIdleConns := getEnvAsInt("DB_MAX_IDLE_CONNS", 10)
	maxLifetime := time.Duration(getEnvAsInt("DB_CONN_MAX_LIFETIME_SECONDS", 1800)) * time.Second
	maxIdleTime := time.Duration(getEnvAsInt("DB_CONN_MAX_IDLE_TIME_SECONDS", 300)) * time.Second
	return openEventStore(databaseURL, maxOpenConns, maxIdleConns, maxLifetime, maxIdleTime)
}
