package queue_nats

import (
	"github.com/infrago/queue"
)

func Driver() queue.Driver {
	return &natsDriver{}
}

func JsDriver() queue.Driver {
	return &natsjsDriver{}
}

func init() {
	jsd := JsDriver()
	queue.Register("nats", Driver())
	queue.Register("najs", jsd)
	queue.Register("natsjs", jsd)
	queue.Register("nats-js", jsd)
}
