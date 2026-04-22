package main

import "log/slog"

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
			if existing, ok := h.clients[client.pubKey]; ok {
				close(existing.send)
				slog.Info("Kicked previous connection", "pubkey", client.pubKey[:8]+"...")
			}
			h.clients[client.pubKey] = client
			slog.Info("User connected", "pubkey", client.pubKey[:8]+"...")

			offlineMsgs, err := getAndDeleteOfflineMessages(client.pubKey)
			if err != nil {
				slog.Error("Failed to fetch offline messages", "pubkey", client.pubKey[:8]+"...", "error", err)
			} else if len(offlineMsgs) > 0 {
				slog.Info("Delivering offline messages", "pubkey", client.pubKey[:8]+"...", "count", len(offlineMsgs))
				for _, msg := range offlineMsgs {
					client.send <- msg
				}
			}

		case client := <-h.unregister:
			if existing, ok := h.clients[client.pubKey]; ok && existing == client {
				delete(h.clients, client.pubKey)
				close(client.send)
				slog.Info("User disconnected", "pubkey", client.pubKey[:8]+"...")
			}

		case message := <-h.broadcast:
			recipientClient, isOnline := h.clients[message.RecipientPubKey]
			senderClient, senderOnline := h.clients[message.SenderPubKey]

			if isOnline {
				select {
				case recipientClient.send <- message:
					slog.Debug("Routed message", "to", message.RecipientPubKey[:8]+"...")
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
				slog.Debug("Recipient offline, queuing message", "to", message.RecipientPubKey[:8]+"...")
				if err := saveOfflineMessage(message); err != nil {
					slog.Error("Failed to save offline message", "to", message.RecipientPubKey[:8]+"...", "error", err)
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
						slog.Debug("Routed group message", "to", recipient.PubKey[:8]+"...")
					default:
						close(recipientClient.send)
						delete(h.clients, recipient.PubKey)
						if err := saveOfflineMessage(msg); err != nil {
							slog.Error("Failed to queue group message", "to", recipient.PubKey[:8]+"...", "error", err)
							allQueued = false
						}
					}
				} else {
					if err := saveOfflineMessage(msg); err != nil {
						slog.Error("Failed to queue group message", "to", recipient.PubKey[:8]+"...", "error", err)
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
