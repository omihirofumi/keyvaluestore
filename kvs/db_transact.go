package kvs

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
)

const (
	TABLE_NAME = "transaction_log"
)

type PostgresTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db     *sql.DB
}

func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

type PostgresDBParams struct {
	dbName   string
	host     string
	port     uint32
	user     string
	password string
	sslmode  string
}

func NewPostgresTransactionLogger(config PostgresDBParams) (*PostgresTransactionLogger, error) {
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		config.host, config.port, config.dbName, config.user, config.password, config.sslmode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	logger := &PostgresTransactionLogger{db: db}

	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}
	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}
	return logger, nil
}

func (l *PostgresTransactionLogger) Run() {
	events := make(chan Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		query := `
			INSERT INTO transaction_log
			(event_type, key, value)
			VALUES ($1, $2, $3)`

		for e := range events {
			_, err := l.db.Exec(query, e.EventType, e.Key, e.Value)
			if err != nil {
				errors <- err
			}
		}
	}()
}

func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		defer close(outEvent)
		defer close(outError)

		query := `SELECT sequence, event_type, key, value FROM transaction_log
					ORDER BY sequence`

		rows, err := l.db.Query(query)
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}
		defer rows.Close()
		e := Event{}

		for rows.Next() {
			err = rows.Scan(&e.Sequence, &e.EventType, &e.Key, &e.Value)
			if err != nil {
				outError <- fmt.Errorf("error reading row: %w", err)
				return
			}

			outEvent <- e
		}

		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
	}()
	return outEvent, outError
}

func (l *PostgresTransactionLogger) Close() error {
	if l.events != nil {
		close(l.events) // Terminates Run loop and goroutine
	}

	return l.db.Close()
}

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	var isExist bool
	stmt := fmt.Sprintf(
		`SELECT EXISTS (
				SELECT 1 
				FROM information_schema.tables 
				WHERE table_name = '%s'
			)`, TABLE_NAME)

	err := l.db.QueryRow(stmt).Scan(&isExist)
	if err != nil {
		return false, err
	}
	return isExist, nil
}

func (l *PostgresTransactionLogger) createTable() error {
	cmd := fmt.Sprintf(`
		CREATE TABLE %s (
		  sequence serial PRIMARY KEY,
		  event_type SMALLINT,
		  key VARCHAR(255) NOT NULL,
		  value VARCHAR(255)
		);`, TABLE_NAME)

	_, err := l.db.Exec(cmd)
	return err
}
