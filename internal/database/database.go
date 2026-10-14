package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"database/sql"
	"github.com/Rezaft2013/event-real-time-processor/internal/model"
	"github.com/lib/pq"
)

func InsertEventToDB(evt *model.Event, validationErr error) error {
	connStr := "host=postgres_db port=5432 user=myuser password=mypassword dbname=events sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Printf("error opening database connection: %v", err)
		return err
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Printf("database unreachable: %v", err)
		return err
	}

	fmt.Println("Successfully connected to PostgreSQL!")
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}

	defer tx.Rollback()

	query1 := `INSERT INTO events_global VALUES ($1)`
	if _, err := tx.ExecContext(ctx, query1, evt.ID); err != nil {
		if pgErr, ok := err.(*pq.Error); ok && pgErr.Code == "23505" {
			log.Printf("duplicate event %v. skipping.", evt)
			return nil
		} else {
			log.Println("failed to insert event %v into events_global, err = %s", evt, err)
			return fmt.Errorf("failed to insert event %v into events_global", evt)
		}
	}

	query2 := `INSERT INTO events_partitioned (id, event_type, event_version, tenant_id, payload, event_timestamp, valid_schema, schema_validation_error) 
			               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	evtTimestamp := time.Unix(evt.Timestamp, 0)
	validationErrDB := sql.NullString{String: "", Valid: false}
	if validationErr != nil {
		validationErrDB.Valid = true
		validationErrDB.String = validationErr.Error()
	}
	_, err = tx.Exec(query2, evt.ID, evt.Type, evt.Version, evt.TenantID, evt.Payload, evtTimestamp, validationErr == nil, validationErrDB)
	if err != nil {
		log.Printf("failed to insert into events_partitioned: %v", err)
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit tx: %w", err)
	}

	fmt.Printf("successfully inserted event %v to database.\n", evt)
	return nil
}
