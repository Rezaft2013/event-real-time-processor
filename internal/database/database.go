package database

import (
	"log"
	"fmt"
	"context"
	"time"

	"database/sql"
	"github.com/lib/pq" 
	"github.com/Rezaft2013/event-real-time-processor/internal/model"
)

func InsertToDB(evt *model.Event) error{
	connStr := "host=postgres_db port=5432 user=myuser password=mypassword dbname=events sslmode=disable"	
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Printf("Error opening database connection: %v", err)
		return err
	}
	defer db.Close()

	
	err = db.Ping()
	if err != nil {
		log.Printf("Database unreachable: %v", err)
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
			log.Printf("Duplicate event %v. skipping.", evt)
			return nil
		} else {
			log.Println("failed to insert event %v into events_global, err = %s", evt, err)
			return fmt.Errorf("failed to insert event %v into events_global", evt)
		}		
	}

	query2 := `INSERT INTO events_partitioned (id, event_type, event_version, tenant_id, payload, created_at) 
			               VALUES ($1, $2, $3, $4, $5, $6)`
	
	evtTimestamp := time.Unix(evt.Timestamp, 0)
	_, err = tx.Exec(query2, evt.ID, evt.Type, evt.Version, evt.TenantID, evt.Payload, evtTimestamp)
	if err != nil {
		log.Printf("Failed to insert into events_partitioned: %v", err)
		return err
	}
		
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit tx: %w", err)
	}

	fmt.Printf("Successfully inserted event %v to database.\n", evt)	
	return nil
}