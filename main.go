package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/valyala/fasthttp"
)

// Global NATS connection and message buffer
var (
	nc        *nats.Conn
	msgBuffer = make(map[string][]string)
	mu        sync.RWMutex
)

// Message represents a message to be published to a NATS channel
type Message struct {
	Message string `json:"message"`
	Channel string `json:"channel"`
}

// Initialize NATS connection
func initNATS() error {
	var err error
	nc, err = nats.Connect(nats.DefaultURL)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %v", err)
	}
	return nil
}

// Start subscription for a client on a given channel to gather messages asynchronously
func startSubscription(client, channel string) {
	subKey := fmt.Sprintf("%s:%s", client, channel)

	// Subscribe to the channel with a queue group to avoid duplicate messages for each client
	sub, err := nc.QueueSubscribe(channel, subKey, func(msg *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()
		msgBuffer[subKey] = append(msgBuffer[subKey], string(msg.Data))
	})
	if err != nil {
		log.Printf("Error subscribing to channel %s: %v", channel, err)
		return
	}
	sub.SetPendingLimits(-1, -1) // Set unlimited pending message limits to avoid message drop
}

// PostHandler handles HTTP POST requests to publish messages to NATS
func PostHandler(ctx *fasthttp.RequestCtx) {
	// Parse JSON payload
	var messages []Message
	if err := json.Unmarshal(ctx.PostBody(), &messages); err != nil {
		ctx.Error("Bad Request", fasthttp.StatusBadRequest)
		return
	}

	// Publish each message to the specified channel
	for _, msg := range messages {
		if err := nc.Publish(msg.Channel, []byte(msg.Message)); err != nil {
			ctx.Error("Failed to publish message", fasthttp.StatusInternalServerError)
			return
		}
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBodyString("Messages published successfully")
}

// GetHandler handles HTTP GET requests to retrieve unread messages for a client on a channel
func GetHandler(ctx *fasthttp.RequestCtx) {
	channel := string(ctx.QueryArgs().Peek("channel"))
	client := string(ctx.QueryArgs().Peek("client"))

	if channel == "" || client == "" {
		ctx.Error("Bad Request: missing 'channel' or 'client' parameter", fasthttp.StatusBadRequest)
		return
	}

	// Simulate authorization check (add your custom logic here)
	if !isAuthorized(client, channel) {
		ctx.Error("Unauthorized", fasthttp.StatusUnauthorized)
		return
	}

	// Initialize subscription for the client if it doesn’t already exist
	subKey := fmt.Sprintf("%s:%s", client, channel)
	mu.RLock()
	if _, exists := msgBuffer[subKey]; !exists {
		mu.RUnlock()
		mu.Lock()
		if _, exists := msgBuffer[subKey]; !exists { // double-checking
			msgBuffer[subKey] = []string{}
			startSubscription(client, channel)
		}
		mu.Unlock()
	} else {
		mu.RUnlock()
	}

	// Retrieve unread messages from the buffer for this client-channel
	mu.Lock()
	messages := msgBuffer[subKey]
	msgBuffer[subKey] = nil // Clear buffer after retrieval
	mu.Unlock()

	// Send unread messages as JSON array
	ctx.SetStatusCode(fasthttp.StatusOK)
	if len(messages) > 0 {
		ctx.SetContentType("application/json")
		json.NewEncoder(ctx).Encode(messages)
	} else {
		ctx.SetBodyString("No new messages")
	}
}

// Simulate client authorization for channels
func isAuthorized(client, channel string) bool {
	// Implement your custom authorization logic here
	return true // Allow all for this example
}

func main() {
	// Initialize NATS connection
	if err := initNATS(); err != nil {
		log.Fatalf("Error initializing NATS: %v", err)
	}
	defer nc.Close()

	// Define HTTP server with fasthttp
	server := fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			switch string(ctx.Path()) {
			case "/post":
				if ctx.IsPost() {
					PostHandler(ctx)
				} else {
					ctx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
				}
			case "/get":
				if ctx.IsGet() {
					GetHandler(ctx)
				} else {
					ctx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
				}
			default:
				ctx.Error("Not Found", fasthttp.StatusNotFound)
			}
		},
	}

	// Start HTTP server
	log.Println("Starting HTTP server on :8080")
	if err := server.ListenAndServe(":8080"); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
