package main

import "log"

type Hub struct {
	clients        map[string]*Client
	broadcast      chan ChatMessage
	groupBroadcast chan GroupChatMessage
	register       chan *Client
	unregister     chan *Client
}

func newHub() *Hub {
	return &Hub{
		broadcast:      make(chan ChatMessage),
		groupBroadcast: make(chan GroupChatMessage),
		register:       make(chan *Client),
		unregister:     make(chan *Client),
		clients:        make(map[string]*Client),
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

		case gmsg := <-h.groupBroadcast:
			senderClient, senderOnline := h.clients[gmsg.SenderPubKey]
			allQueued := true

			for _, recipient := range gmsg.Recipients {
				msg := ChatMessage{
					Type:            "message",
					MessageID:       gmsg.MessageID,
					GroupID:         gmsg.GroupID,
					SenderPubKey:    gmsg.SenderPubKey,
					RecipientPubKey: recipient.PubKey,
					EncryptedBlob:   recipient.EncryptedBlob,
				}

				recipientClient, isOnline := h.clients[recipient.PubKey]
				if isOnline {
					select {
					case recipientClient.send <- msg:
						log.Printf("Routed group message to: %s", recipient.PubKey[:8]+"...")
					default:
						close(recipientClient.send)
						delete(h.clients, recipient.PubKey)
						if err := saveOfflineMessage(msg); err != nil {
							log.Printf("CRITICAL: Failed to queue group message: %v", err)
							allQueued = false
						}
					}
				} else {
					if err := saveOfflineMessage(msg); err != nil {
						log.Printf("CRITICAL: Failed to queue group message for %s: %v", recipient.PubKey[:8]+"...", err)
						allQueued = false
					}
				}
			}

			if senderOnline && allQueued {
				senderClient.send <- ChatMessage{
					Type:      "ack",
					MessageID: gmsg.MessageID,
				}
			}
		}
	}
}
