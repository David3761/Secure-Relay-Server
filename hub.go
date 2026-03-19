package main

import "log"

type Hub struct {
	clients    map[string]*Client
	broadcast  chan ChatMessage
	register   chan *Client
	unregister chan *Client
}

func newHub() *Hub {
	return &Hub{
		broadcast:  make(chan ChatMessage),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[string]*Client),
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client.pubKey] = client
			log.Printf("User connected: %s", client.pubKey[:8]+"...")

			offlineMsgs, err := getAndDeleteOfflineMessages(client.pubKey)
			if err != nil {
				log.Printf("Error fetching offline messages: %v", err)
			} else if len(offlineMsgs) > 0 {
				log.Printf("Delivering %d offline messages to %s", len(offlineMsgs), client.pubKey[:8]+"...")
				for _, msg := range offlineMsgs {
					client.send <- msg
				}
			}

		case client := <-h.unregister:
			if _, ok := h.clients[client.pubKey]; ok {
				delete(h.clients, client.pubKey)
				close(client.send)
				log.Printf("User disconnected: %s", client.pubKey[:8]+"...")
			}

		case message := <-h.broadcast:
			recipientClient, isOnline := h.clients[message.RecipientPubKey]

			senderClient, senderOnline := h.clients[message.SenderPubKey]

			if isOnline {
				select {
				case recipientClient.send <- message:
					log.Printf("Routed message instantly to: %s", message.RecipientPubKey[:8]+"...")
					if senderOnline {
						senderClient.send <- ChatMessage{
							Type:      "ack",
							MessageID: message.MessageID,
						}
					}
				default:
					close(recipientClient.send)
					delete(h.clients, recipientClient.pubKey)
				}
			} else {
				log.Printf("Recipient offline. Queuing message for: %s", message.RecipientPubKey[:8]+"...")
				err := saveOfflineMessage(message)
				if err != nil {
					log.Printf("CRITICAL: Failed to save offline message: %v", err)
				} else if senderOnline {
					senderClient.send <- ChatMessage{
						Type:      "ack",
						MessageID: message.MessageID,
					}
				}
			}
		}
	}
}
