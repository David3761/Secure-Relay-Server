package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
)

type ChatMessage struct {
	Type            string `json:"type"`
	MessageID       string `json:"message_id"`
	SenderPubKey    string `json:"sender_pub_key"`
	RecipientPubKey string `json:"recipient_pub_key"`
	EncryptedBlob   string `json:"encrypted_blob"`
	GroupID         string `json:"group_id,omitempty"`
}

type GroupRecipient struct {
	PubKey        string `json:"pub_key"`
	EncryptedBlob string `json:"encrypted_blob"`
}

type GroupChatMessage struct {
	Type         string           `json:"type"`
	MessageID    string           `json:"message_id"`
	GroupID      string           `json:"group_id"`
	SenderPubKey string           `json:"sender_pub_key"`
	Recipients   []GroupRecipient `json:"recipients"`
}

func main() {
	_ = godotenv.Load()

	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	)

	initDB(connString)

	rl = newRateLimiter()

	go func() {
		ticker := time.NewTicker(time.Hour)
		for range ticker.C {
			if err := deleteExpiredMessages(); err != nil {
				log.Printf("Failed to expire offline messages: %v", err)
			}
		}
	}()

	hub := newHub()
	go hub.run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	log.Println("Secure Relay Server starting on :8080...")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
