package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/segmentio/ksuid"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := mainE(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		// Only exit non-zero if our initial context has yet to be canceled.
		// Otherwise it's very likely that the error we're seeing is a result of our attempt at graceful shutdown.
		if ctx.Err() == nil {
			os.Exit(1)
		}
	}
}
func mainE(_ context.Context) error {
	n := maelstrom.NewNode()
	h := Handler{
		node: n,
	}
	n.Handle("echo", h.Echo)
	n.Handle("generate", h.Generate)
	n.Handle("broadcast", h.Broadcast)
	n.Handle("read", h.Read)
	n.Handle("topology", h.Topology)
	return n.Run()
}

type Handler struct {
	node *maelstrom.Node

	messageMu sync.RWMutex
	messages  []int

	topologyMu sync.RWMutex
	neighbors  []string
}

func (h *Handler) Echo(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "echo_ok"

	return h.node.Reply(msg, body)
}

func (h *Handler) Generate(msg maelstrom.Message) error {
	body := struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}{
		Type: "generate_ok",
		ID:   ksuid.New().String(),
	}
	return h.node.Reply(msg, body)
}

func (h *Handler) Broadcast(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
	}

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("parse error: %w", err)
	}
	h.messageMu.Lock()
	h.messages = append(h.messages, body.Message)
	h.messageMu.Unlock()

	type resp struct {
		Type string `json:"type"`
	}
	return h.node.Reply(msg, resp{Type: "broadcast_ok"})
}

func (h *Handler) Read(msg maelstrom.Message) error {
	type resp struct {
		Type     string `json:"type"`
		Messages []int  `json:"messages"`
	}
	h.messageMu.RLock()
	messages := make([]int, len(h.messages))
	copy(messages, h.messages)
	h.messageMu.RUnlock()

	return h.node.Reply(msg, resp{Type: "read_ok", Messages: messages})
}

func (h *Handler) Topology(msg maelstrom.Message) error {
	type resp struct {
		Type string `json:"type"`
	}
	return h.node.Reply(msg, resp{Type: "topology_ok"})
}
