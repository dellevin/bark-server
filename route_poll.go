package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/finb/bark-server/v2/database"
	"github.com/finb/bark-server/v2/queue"
	"github.com/gofiber/fiber/v2"
)

// longPollWait is how long a /fetch request is held open waiting for a message.
// It must stay below the fiber write timeout (configured to 30s in main.go).
const longPollWait = 20 * time.Second

// messageQueue is initialised in runServer before the router starts serving.
var messageQueue *queue.Store

func init() {
	registerRoute("poll", func(router fiber.Router) {
		router.Get("/fetch", handleFetch)
		router.Post("/ack", handleAck)
		router.Get("/queue", handleQueueLen)
	})
}

// validateAndroidDevice resolves a device key and confirms it is an android
// device (the only type allowed to open a WebSocket or long-poll the queue).
func validateAndroidDevice(deviceKey string) (*database.DeviceRecord, error) {
	if deviceKey == "" {
		return nil, errors.New("device key is empty")
	}
	record, err := db.DeviceTokenByKey(deviceKey)
	if err != nil {
		return nil, err
	}
	if record.Type != database.DeviceTypeAndroid {
		return nil, fmt.Errorf("device %q is type %q, not android", deviceKey, record.Type)
	}
	return record, nil
}

// handleFetch serves one message via long polling. It returns 204 when no
// message arrives within longPollWait.
func handleFetch(c *fiber.Ctx) error {
	deviceKey := c.Query("device_key")
	if _, err := validateAndroidDevice(deviceKey); err != nil {
		return c.Status(400).JSON(failed(400, "%s", err.Error()))
	}

	env, err := messageQueue.Fetch(c.UserContext(), deviceKey, longPollWait)
	if err != nil {
		return c.Status(500).JSON(failed(500, "fetch failed: %v", err))
	}
	if env == nil {
		return c.SendStatus(fiber.StatusNoContent)
	}
	return c.JSON(env)
}

// ackRequest accepts the device key and message id in JSON or query params.
type ackRequest struct {
	DeviceKey string `json:"device_key" query:"device_key"`
	ID        string `json:"id" query:"id"`
}

// handleAck confirms a message previously returned by /fetch, removing it from
// the inflight bucket.
func handleAck(c *fiber.Ctx) error {
	var req ackRequest
	// Body is optional; ignore the parse error when the client uses query params.
	_ = c.BodyParser(&req)
	if req.DeviceKey == "" {
		req.DeviceKey = c.Query("device_key")
	}
	if req.ID == "" {
		req.ID = c.Query("id")
	}
	if req.DeviceKey == "" || req.ID == "" {
		return c.Status(400).JSON(failed(400, "device_key and id are required"))
	}
	if err := messageQueue.Ack(req.DeviceKey, req.ID); err != nil {
		return c.Status(500).JSON(failed(500, "ack failed: %v", err))
	}
	return c.JSON(success())
}

// handleQueueLen reports how many messages are buffered (pending + inflight).
func handleQueueLen(c *fiber.Ctx) error {
	deviceKey := c.Query("device_key")
	if _, err := validateAndroidDevice(deviceKey); err != nil {
		return c.Status(400).JSON(failed(400, "%s", err.Error()))
	}
	n, err := messageQueue.Len(deviceKey)
	if err != nil {
		return c.Status(500).JSON(failed(500, "queue query failed: %v", err))
	}
	return c.JSON(data(fiber.Map{"device_key": deviceKey, "queued": n}))
}
