package queue

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

func testEnvelope(id string) Envelope {
	return Envelope{
		Type:      "message",
		ID:        id,
		Data:      json.RawMessage(`{"body":"` + id + `"}`),
		Timestamp: time.Now().Unix(),
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), time.Hour)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestEnqueueFetchIsFIFO(t *testing.T) {
	s := newTestStore(t)
	const dev = "dev1"
	for _, id := range []string{"a", "b", "c"} {
		if err := s.Enqueue(dev, testEnvelope(id)); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"a", "b", "c"} {
		env, err := s.Fetch(context.Background(), dev, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if env == nil || env.ID != want {
			t.Fatalf("want %s, got %v", want, env)
		}
	}
}

func TestFetchOnEmptyTimesOut(t *testing.T) {
	s := newTestStore(t)
	start := time.Now()
	env, err := s.Fetch(context.Background(), "nobody", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if env != nil {
		t.Fatalf("expected nil envelope on timeout, got %#v", env)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("Fetch returned before the long-poll wait elapsed")
	}
}

func TestLongPollWakesOnEnqueue(t *testing.T) {
	s := newTestStore(t)
	const dev = "dev2"
	errCh := make(chan error, 1)
	envCh := make(chan *Envelope, 1)
	go func() {
		env, err := s.Fetch(context.Background(), dev, 5*time.Second)
		errCh <- err
		envCh <- env
	}()

	time.Sleep(100 * time.Millisecond) // let the goroutine park on the waiter
	if err := s.Enqueue(dev, testEnvelope("wake")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
		env := <-envCh
		if env == nil || env.ID != "wake" {
			t.Fatalf("unexpected envelope: %#v", env)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("long poll was not woken by Enqueue")
	}
}

func TestAckRemovesInflight(t *testing.T) {
	s := newTestStore(t)
	const dev = "dev3"
	if err := s.Enqueue(dev, testEnvelope("x")); err != nil {
		t.Fatal(err)
	}
	env, _ := s.Fetch(context.Background(), dev, time.Second)
	if env == nil {
		t.Fatal("expected envelope")
	}
	if n, _ := s.Len(dev); n != 1 {
		t.Fatalf("want len 1 while inflight, got %d", n)
	}
	if err := s.Ack(dev, env.ID); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Len(dev); n != 0 {
		t.Fatalf("want len 0 after ack, got %d", n)
	}
}

// expireLease forces an inflight message's lease into the past so the next
// Fetch treats it as abandoned and requeues it.
func expireLease(s *Store, dev, id string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		inflight := tx.Bucket([]byte(rootBucket)).
			Bucket([]byte(dev)).
			Bucket([]byte(inflightBucket))
		v := inflight.Get([]byte(id))
		if v == nil {
			return nil
		}
		var rec stored
		if err := json.Unmarshal(v, &rec); err != nil {
			return err
		}
		rec.LeaseUntil = 1
		out, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return inflight.Put([]byte(id), out)
	})
}

func TestExpiredLeaseIsRequeued(t *testing.T) {
	s := newTestStore(t)
	const dev = "dev4"
	if err := s.Enqueue(dev, testEnvelope("r")); err != nil {
		t.Fatal(err)
	}
	env, _ := s.Fetch(context.Background(), dev, time.Second)
	if env == nil || env.ID != "r" {
		t.Fatalf("unexpected first fetch: %#v", env)
	}
	if err := expireLease(s, dev, env.ID); err != nil {
		t.Fatal(err)
	}

	again, err := s.Fetch(context.Background(), dev, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if again == nil || again.ID != "r" {
		t.Fatalf("expired lease should be redelivered, got %#v", again)
	}
}

func TestDurabilityAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Enqueue("dev5", testEnvelope("persist")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	env, err := s2.Fetch(context.Background(), "dev5", time.Second)
	if err != nil || env == nil || env.ID != "persist" {
		t.Fatalf("durable message lost: env=%v err=%v", env, err)
	}
}
