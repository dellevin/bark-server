// Package queue persists push messages destined for offline Android devices
// in a bbolt database and serves them through HTTP long polling.
//
// Each device gets its own nested bucket with two children:
//   - "pending":  messages not yet delivered, keyed by an increasing sequence
//     so they are returned FIFO.
//   - "inflight": messages handed out via Fetch but not yet acked; each has a
//     lease deadline and is requeued once the lease expires.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mritd/logger"
	"go.etcd.io/bbolt"
)

const (
	rootBucket     = "bark_queue"
	pendingBucket  = "pending"
	inflightBucket = "inflight"

	// DefaultTTL is how long an undelivered message is retained.
	DefaultTTL = 7 * 24 * time.Hour
	// leaseDuration is how long a fetched message waits for an ack before it
	// becomes visible again to another Fetch.
	leaseDuration = 60 * time.Second
	// janitorInterval controls how often expired messages are swept.
	janitorInterval = time.Hour
	// seqKeyWidth zero-pads sequence numbers so bucket iteration stays FIFO.
	seqKeyWidth = 20
)

// ErrClosed is returned when the store has been shut down.
var ErrClosed = errors.New("queue: store closed")

// Envelope is the wire format of a queued message. Data holds the raw JSON of
// the push payload so the queue never needs to understand its contents.
type Envelope struct {
	Type      string          `json:"type"`
	ID        string          `json:"id"`
	Data      json.RawMessage `json:"data"`
	Timestamp int64           `json:"timestamp"`
}

// stored is the on-disk representation, adding bookkeeping fields.
type stored struct {
	Envelope
	EnqueuedAt int64 `json:"enqueued_at"`
	LeaseUntil int64 `json:"lease_until,omitempty"`
}

// Store is a bbolt-backed durable queue.
type Store struct {
	db  *bbolt.DB
	ttl time.Duration

	mu      sync.Mutex
	waiters map[string][]chan struct{}

	stop chan struct{}
	done chan struct{}
}

// Open creates or opens the queue database inside dataDir and starts the TTL
// janitor goroutine.
func Open(dataDir string, ttl time.Duration) (*Store, error) {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("queue: create data dir: %w", err)
	}
	db, err := bbolt.Open(filepath.Join(dataDir, "queue.db"), 0o600, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("queue: open db: %w", err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(rootBucket))
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}

	s := &Store{
		db:      db,
		ttl:     ttl,
		waiters: make(map[string][]chan struct{}),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go s.runJanitor()
	return s, nil
}

// Close stops the janitor and closes the database.
func (s *Store) Close() error {
	close(s.stop)
	<-s.done
	s.mu.Lock()
	for _, chs := range s.waiters {
		for _, ch := range chs {
			close(ch)
		}
	}
	s.waiters = nil
	s.mu.Unlock()
	return s.db.Close()
}

// Enqueue persists a message for the device and wakes any blocked Fetch.
func (s *Store) Enqueue(deviceKey string, env Envelope) error {
	if deviceKey == "" {
		return errors.New("queue: empty device key")
	}
	record := stored{Envelope: env, EnqueuedAt: time.Now().Unix()}
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}

	err = s.db.Update(func(tx *bbolt.Tx) error {
		root, err := tx.CreateBucketIfNotExists([]byte(rootBucket))
		if err != nil {
			return err
		}
		dev, err := root.CreateBucketIfNotExists([]byte(deviceKey))
		if err != nil {
			return err
		}
		pending, err := dev.CreateBucketIfNotExists([]byte(pendingBucket))
		if err != nil {
			return err
		}
		if _, err := dev.CreateBucketIfNotExists([]byte(inflightBucket)); err != nil {
			return err
		}
		seq, err := pending.NextSequence()
		if err != nil {
			return err
		}
		return pending.Put([]byte(fmt.Sprintf("%0*d", seqKeyWidth, seq)), payload)
	})
	if err != nil {
		return err
	}
	s.notify(deviceKey)
	return nil
}

// Fetch returns the next available message, moving it to inflight. When the
// queue is empty it blocks up to maxWait for a new Enqueue, returning nil
// (caller responds 204) on timeout. Expired inflight leases are requeued first.
func (s *Store) Fetch(ctx context.Context, deviceKey string, maxWait time.Duration) (*Envelope, error) {
	if env, ok, err := s.tryFetch(deviceKey); err != nil || ok {
		return env, err
	}

	ch, cleanup := s.addWaiter(deviceKey)
	defer cleanup()

	// Re-check after registering: a message may have arrived in the gap.
	if env, ok, err := s.tryFetch(deviceKey); err != nil || ok {
		return env, err
	}

	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	select {
	case <-ch:
		env, _, err := s.tryFetch(deviceKey)
		return env, err
	case <-timer.C:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.stop:
		return nil, ErrClosed
	}
}

// tryFetch requeues stale leases, then pops the oldest pending message into
// inflight with a fresh lease.
func (s *Store) tryFetch(deviceKey string) (*Envelope, bool, error) {
	var result *Envelope
	err := s.db.Update(func(tx *bbolt.Tx) error {
		dev := tx.Bucket([]byte(rootBucket)).Bucket([]byte(deviceKey))
		if dev == nil {
			return nil
		}
		if err := s.requeueExpired(dev); err != nil {
			return err
		}
		pending := dev.Bucket([]byte(pendingBucket))
		inflight := dev.Bucket([]byte(inflightBucket))
		k, v := pending.Cursor().First()
		if k == nil {
			return nil
		}
		record := stored{}
		if err := json.Unmarshal(v, &record); err != nil {
			// Drop corrupt records instead of blocking the queue forever.
			return pending.Delete(k)
		}
		now := time.Now().Unix()
		record.LeaseUntil = now + int64(leaseDuration/time.Second)
		updated, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := inflight.Put([]byte(record.ID), updated); err != nil {
			return err
		}
		if err := pending.Delete(k); err != nil {
			return err
		}
		env := record.Envelope
		result = &env
		return nil
	})
	if err != nil || result == nil {
		return nil, false, err
	}
	return result, true, nil
}

// Ack removes a fetched message from inflight. An unknown id is not an error.
func (s *Store) Ack(deviceKey, messageID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		dev := tx.Bucket([]byte(rootBucket)).Bucket([]byte(deviceKey))
		if dev == nil {
			return nil
		}
		inflight := dev.Bucket([]byte(inflightBucket))
		if inflight == nil {
			return nil
		}
		return inflight.Delete([]byte(messageID))
	})
}

// Len returns the number of pending + inflight messages for the device.
func (s *Store) Len(deviceKey string) (int, error) {
	count := 0
	err := s.db.View(func(tx *bbolt.Tx) error {
		dev := tx.Bucket([]byte(rootBucket)).Bucket([]byte(deviceKey))
		if dev == nil {
			return nil
		}
		if b := dev.Bucket([]byte(pendingBucket)); b != nil {
			count += b.Stats().KeyN
		}
		if b := dev.Bucket([]byte(inflightBucket)); b != nil {
			count += b.Stats().KeyN
		}
		return nil
	})
	return count, err
}

// requeueExpired moves inflight messages whose lease has lapsed back to
// pending. Must be called inside a write transaction.
func (s *Store) requeueExpired(dev *bbolt.Bucket) error {
	inflight := dev.Bucket([]byte(inflightBucket))
	pending := dev.Bucket([]byte(pendingBucket))
	if inflight == nil || pending == nil {
		return nil
	}
	now := time.Now().Unix()
	var stale [][]byte
	if err := inflight.ForEach(func(k, v []byte) error {
		record := stored{}
		if err := json.Unmarshal(v, &record); err != nil {
			stale = append(stale, k) // corrupt inflight entry: drop
			return nil
		}
		if record.LeaseUntil > 0 && record.LeaseUntil <= now {
			stale = append(stale, k)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, k := range stale {
		v := inflight.Get(k)
		if v == nil {
			continue
		}
		record := stored{}
		if err := json.Unmarshal(v, &record); err != nil {
			_ = inflight.Delete(k)
			continue
		}
		record.LeaseUntil = 0
		updated, err := json.Marshal(record)
		if err != nil {
			return err
		}
		seq, err := pending.NextSequence()
		if err != nil {
			return err
		}
		if err := pending.Put([]byte(fmt.Sprintf("%0*d", seqKeyWidth, seq)), updated); err != nil {
			return err
		}
		if err := inflight.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) addWaiter(deviceKey string) (chan struct{}, func()) {
	ch := make(chan struct{})
	s.mu.Lock()
	if s.waiters == nil {
		s.mu.Unlock()
		return ch, func() {}
	}
	s.waiters[deviceKey] = append(s.waiters[deviceKey], ch)
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.waiters == nil {
			return
		}
		chs := s.waiters[deviceKey]
		for i, w := range chs {
			if w == ch {
				s.waiters[deviceKey] = append(chs[:i], chs[i+1:]...)
				break
			}
		}
		if len(s.waiters[deviceKey]) == 0 {
			delete(s.waiters, deviceKey)
		}
	}
}

func (s *Store) notify(deviceKey string) {
	s.mu.Lock()
	chs := s.waiters[deviceKey]
	delete(s.waiters, deviceKey)
	s.mu.Unlock()
	for _, ch := range chs {
		close(ch)
	}
}

func (s *Store) runJanitor() {
	defer close(s.done)
	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			if err := s.sweepExpired(); err != nil {
				logger.Errorf("queue janitor sweep failed: %v", err)
			}
		}
	}
}

// sweepExpired deletes messages older than ttl in both pending and inflight.
func (s *Store) sweepExpired() error {
	cutoff := time.Now().Add(-s.ttl).Unix()
	return s.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte(rootBucket))
		if root == nil {
			return nil
		}
		return root.ForEach(func(devKey, _ []byte) error {
			dev := root.Bucket(devKey)
			if dev == nil {
				return nil
			}
			for _, childName := range [][]byte{[]byte(pendingBucket), []byte(inflightBucket)} {
				child := dev.Bucket(childName)
				if child == nil {
					continue
				}
				var dead [][]byte
				if err := child.ForEach(func(k, v []byte) error {
					record := stored{}
					if json.Unmarshal(v, &record) != nil {
						dead = append(dead, k)
						return nil
					}
					if record.EnqueuedAt > 0 && record.EnqueuedAt < cutoff {
						dead = append(dead, k)
					}
					return nil
				}); err != nil {
					return err
				}
				for _, k := range dead {
					if err := child.Delete(k); err != nil {
						return err
					}
				}
			}
			return nil
		})
	})
}
