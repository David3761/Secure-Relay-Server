package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var db *pgxpool.Pool

func initDB(connString string) {
	var err error
	db, err = pgxpool.New(context.Background(), connString)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}

	if err := db.Ping(context.Background()); err != nil {
		log.Fatalf("Database ping failed: %v\n", err)
	}

	log.Println("Connected to PostgreSQL successfully.")
	ensureSchema()
}

func ensureSchema() {
	query := `
	CREATE TABLE IF NOT EXISTS users (
		public_key VARCHAR(64) PRIMARY KEY,
		last_seen TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS offline_messages (
		id SERIAL PRIMARY KEY,
		message_id VARCHAR(64) NOT NULL,
		sender_pub_key VARCHAR(64) NOT NULL REFERENCES users(public_key),
		recipient_pub_key VARCHAR(64) NOT NULL REFERENCES users(public_key),
		encrypted_blob TEXT NOT NULL,
		group_id TEXT,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	ALTER TABLE offline_messages ADD COLUMN IF NOT EXISTS group_id TEXT;

	CREATE INDEX IF NOT EXISTS idx_recipient ON offline_messages(recipient_pub_key);
	`
	_, err := db.Exec(context.Background(), query)
	if err != nil {
		log.Fatalf("Failed to create schema: %v\n", err)
	}
	log.Println("Database schema verified.")
}

func registerUser(pubKey string) error {
	query := `
		INSERT INTO users (public_key, last_seen) 
		VALUES ($1, $2) 
		ON CONFLICT (public_key) 
		DO UPDATE SET last_seen = EXCLUDED.last_seen;
	`
	_, err := db.Exec(context.Background(), query, pubKey, time.Now())
	return err
}

func saveOfflineMessage(msg ChatMessage) error {
	registerUser(msg.SenderPubKey)
	registerUser(msg.RecipientPubKey)

	var groupID *string
	if msg.GroupID != "" {
		groupID = &msg.GroupID
	}

	query := `
		INSERT INTO offline_messages (message_id, sender_pub_key, recipient_pub_key, encrypted_blob, group_id)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := db.Exec(context.Background(), query, msg.MessageID, msg.SenderPubKey, msg.RecipientPubKey, msg.EncryptedBlob, groupID)
	return err
}

func getAndDeleteOfflineMessages(recipientPubKey string) ([]ChatMessage, error) {
	ctx := context.Background()

	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	query := `
		SELECT message_id, sender_pub_key, encrypted_blob, group_id
		FROM offline_messages
		WHERE recipient_pub_key = $1
		ORDER BY created_at ASC
	`
	rows, err := tx.Query(ctx, query, recipientPubKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []ChatMessage
	for rows.Next() {
		var msg ChatMessage
		msg.Type = "message"
		msg.RecipientPubKey = recipientPubKey

		var groupID *string
		if err := rows.Scan(&msg.MessageID, &msg.SenderPubKey, &msg.EncryptedBlob, &groupID); err != nil {
			return nil, err
		}
		if groupID != nil {
			msg.GroupID = *groupID
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	deleteQuery := `DELETE FROM offline_messages WHERE recipient_pub_key = $1`
	if _, err := tx.Exec(ctx, deleteQuery, recipientPubKey); err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	return messages, err
}
