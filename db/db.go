package db

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"context"
)

func ConnectDB() (*pgxpool.Pool, error) {
	connString := "postgres://root:legion123@localhost:5432/legiondb?sslmode=disable"

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, err
	}

	return pool, nil
}