package queue_nats

import (
	"github.com/infrago/infra"
	"github.com/infrago/queue"
)

func Driver() queue.Driver {
	return &natsDriver{}
}

func JsDriver() queue.Driver {
	return &natsjsDriver{}
}

func init() {
	infra.Register("nats", Driver())

	jsd := JsDriver()
	infra.Register("najs", jsd)
	infra.Register("natsjs", jsd)
	infra.Register("nats-js", jsd)
}
