package main

import (
	"time"

	"github.com/finb/bark-server/v2/wspush"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/mritd/logger"
)

// wsPingInterval keeps idle connections alive through proxies and lets the
// read loop notice a half-open socket when a write fails.
const wsPingInterval = 30 * time.Second

func init() {
	registerRoute("ws", func(router fiber.Router) {
		// Reject anything that is not a WebSocket upgrade request.
		router.Use("/ws", func(c *fiber.Ctx) error {
			if websocket.IsWebSocketUpgrade(c) {
				return c.Next()
			}
			return fiber.ErrUpgradeRequired
		})
		router.Get("/ws", websocket.New(handleWebSocket))
	})
}

// handleWebSocket registers an Android device's live connection in the hub.
// The device key is validated against the database and must be an android
// device; iOS/watchOS devices never open this endpoint.
func handleWebSocket(c *websocket.Conn) {
	deviceKey := c.Query("device_key")
	if deviceKey == "" {
		_ = c.Close()
		return
	}

	if _, err := validateAndroidDevice(deviceKey); err != nil {
		logger.Warnf("ws connection rejected: %v", err)
		_ = c.Close()
		return
	}

	registrationID := wspush.Default.Add(deviceKey, c)
	defer wspush.Default.Remove(deviceKey, registrationID)
	logger.Infof("android device %q connected (registration %d)", deviceKey, registrationID)

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(wsPingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := c.WriteJSON(wspush.Envelope{
					Type:      "ping",
					Timestamp: time.Now().Unix(),
				}); err != nil {
					return
				}
			}
		}
	}()

	// Drain incoming frames (pong/ack) and block until the socket errors or
	// closes, at which point the deferred hub removal unregisters the device.
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			close(done)
			logger.Infof("android device %q disconnected: %v", deviceKey, err)
			return
		}
	}
}
