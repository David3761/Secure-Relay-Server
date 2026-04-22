package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/crypto/nacl/box"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 131072 // 128 KB — enough for ~180 recipients
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return r.Header.Get("X-App-Secret") == os.Getenv("APP_SECRET") },
}

type Client struct {
	hub    *Hub
	conn   *websocket.Conn
	send   chan ChatMessage
	pubKey string
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}

		var base struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &base); err != nil {
			continue
		}

		switch base.Type {
		case "ping", "disconnect", "dummy":
			continue
		case "group_message":
			var gmsg GroupChatMessage
			if err := json.Unmarshal(raw, &gmsg); err != nil {
				log.Printf("Failed to parse group_message: %v", err)
				continue
			}
			if gmsg.SenderPubKey == c.pubKey {
				c.hub.groupBroadcast <- gmsg
			}
		default:
			var msg ChatMessage
			if err := json.Unmarshal(raw, &msg); err != nil {
				log.Printf("Failed to parse message: %v", err)
				continue
			}
			if msg.SenderPubKey == c.pubKey {
				c.hub.broadcast <- msg
			}
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			err := c.conn.WriteJSON(message)
			if err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func isValidHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// authenticate performs a NaCl-box challenge-response handshake over the
// already-upgraded WebSocket connection to verify that the client holds the
// private key corresponding to pubKeyHex.
//
// Protocol:
//  1. Server generates an ephemeral X25519 keypair.
//  2. Server seals a 32-byte random challenge with box.Seal using the client's
//     public key, so only the holder of the matching private key can open it.
//  3. Client decrypts and echoes the raw challenge bytes back as hex.
//  4. Server verifies and resets connection deadlines before returning.
func authenticate(conn *websocket.Conn, pubKeyHex string) bool {
	pubKeyBytes, err := hex.DecodeString(pubKeyHex)
	if err != nil || len(pubKeyBytes) != 32 {
		return false
	}
	var clientPubKey [32]byte
	copy(clientPubKey[:], pubKeyBytes)

	ephPub, ephPriv, err := box.GenerateKey(rand.Reader)
	if err != nil {
		log.Printf("Failed to generate ephemeral keypair: %v", err)
		return false
	}

	challenge := make([]byte, 32)
	if _, err := rand.Read(challenge); err != nil {
		log.Printf("Failed to generate challenge: %v", err)
		return false
	}

	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		log.Printf("Failed to generate nonce: %v", err)
		return false
	}

	sealed := box.Seal(nil, challenge, &nonce, &clientPubKey, ephPriv)

	challengeMsg := map[string]string{
		"type":          "challenge",
		"ephemeral_pub": hex.EncodeToString(ephPub[:]),
		"nonce":         hex.EncodeToString(nonce[:]),
		"sealed":        base64.StdEncoding.EncodeToString(sealed),
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteJSON(challengeMsg); err != nil {
		log.Printf("Failed to send challenge: %v", err)
		return false
	}

	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	var response struct {
		Type     string `json:"type"`
		Solution string `json:"solution"`
	}
	if err := conn.ReadJSON(&response); err != nil {
		log.Printf("Failed to read challenge response: %v", err)
		return false
	}

	if response.Type != "challenge_response" {
		log.Printf("Expected challenge_response, got %q from %s", response.Type, pubKeyHex[:8]+"...")
		return false
	}

	solution, err := hex.DecodeString(response.Solution)
	if err != nil || !bytes.Equal(solution, challenge) {
		log.Printf("Invalid challenge solution from %s", pubKeyHex[:8]+"...")
		return false
	}

	conn.SetReadDeadline(time.Time{})
	conn.SetWriteDeadline(time.Time{})

	log.Printf("Authentication successful for %s", pubKeyHex[:8]+"...")
	return true
}

func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	if !rl.allow(clientIP(r)) {
		http.Error(w, "Too many connection attempts", http.StatusTooManyRequests)
		return
	}

	pubKey := r.URL.Query().Get("pubkey")
	if len(pubKey) != 64 || !isValidHex(pubKey) {
		http.Error(w, "Invalid public key", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade Error:", err)
		return
	}

	if !authenticate(conn, pubKey) {
		conn.Close()
		return
	}

	client := &Client{
		hub:    hub,
		conn:   conn,
		send:   make(chan ChatMessage, 256),
		pubKey: pubKey,
	}

	client.hub.register <- client

	go client.writePump()
	go client.readPump()
}
