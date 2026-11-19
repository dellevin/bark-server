package wspush

import (
	"errors"
	"testing"
)

type fakeConn struct {
	got []interface{}
	err error
}

func (f *fakeConn) WriteJSON(v interface{}) error {
	if f.err != nil {
		return f.err
	}
	f.got = append(f.got, v)
	return nil
}

func TestOfflineDeviceIsNotOnline(t *testing.T) {
	hub := New()
	if hub.Online("missing") {
		t.Fatal("unknown device should be offline")
	}
	if hub.Push("missing", "x") {
		t.Fatal("push to offline device must report not delivered")
	}
}

func TestAddRemoveAndOnlineState(t *testing.T) {
	hub := New()
	id := hub.Add("dev1", &fakeConn{})
	if !hub.Online("dev1") {
		t.Fatal("device should be online after Add")
	}
	hub.Remove("dev1", id)
	if hub.Online("dev1") {
		t.Fatal("device should be offline after Remove")
	}
}

func TestPushDeliversToAllConnections(t *testing.T) {
	hub := New()
	c1 := &fakeConn{}
	c2 := &fakeConn{}
	hub.Add("dev1", c1)
	hub.Add("dev1", c2)

	payload := Envelope{Type: "message", ID: "abc", Data: "hello"}
	if !hub.Push("dev1", payload) {
		t.Fatal("push to online device must report delivered")
	}
	for i, c := range []*fakeConn{c1, c2} {
		if len(c.got) != 1 {
			t.Fatalf("conn %d expected 1 message, got %d", i, len(c.got))
		}
		got, ok := c.got[0].(Envelope)
		if !ok || got.ID != "abc" {
			t.Fatalf("conn %d received unexpected payload: %#v", i, c.got[0])
		}
	}
}

func TestRemoveOneConnectionKeepsOthers(t *testing.T) {
	hub := New()
	c1 := &fakeConn{}
	id1 := hub.Add("dev1", c1)
	hub.Add("dev1", &fakeConn{})

	hub.Remove("dev1", id1)
	if !hub.Online("dev1") {
		t.Fatal("device should stay online while another connection exists")
	}
	if !hub.Push("dev1", "x") {
		t.Fatal("remaining connection should still receive pushes")
	}
}

func TestPushFailsWhenAllWritesError(t *testing.T) {
	hub := New()
	hub.Add("dev1", &fakeConn{err: errors.New("broken pipe")})
	if hub.Push("dev1", "x") {
		t.Fatal("push must report failure when every write errors")
	}
}
