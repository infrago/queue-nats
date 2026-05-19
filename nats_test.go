package queue_nats

import (
	"testing"
	"time"
)

func TestNatsAfterTime(t *testing.T) {
	sec := int64(1773000000)
	if got := natsAfterTime(sec); got.Unix() != sec {
		t.Fatalf("unix=%d, want %d", got.Unix(), sec)
	}

	now := time.Now().Add(time.Minute).Truncate(time.Nanosecond)
	if got := natsAfterTime(now.UnixNano()); !got.Equal(now) {
		t.Fatalf("time=%v, want %v", got, now)
	}
}

func TestJSSubjectAndConsumer(t *testing.T) {
	if got := jsSubject("STREAM", "foo.bar"); got != "STREAM.foo_bar" {
		t.Fatalf("subject=%q", got)
	}
	if got := jsConsumer("STREAM", "foo.bar"); got != "STREAM_foo_bar" {
		t.Fatalf("consumer=%q", got)
	}
}
