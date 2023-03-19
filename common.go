package queue_nats

import (
	"errors"
	"fmt"
	"strings"
)

var (
	errInvalidConnection = errors.New("Invalid queue connection.")
	errAlreadyRunning    = errors.New("Nats queue is already running.")
	errNotRunning        = errors.New("Nats queue is not running.")
)

func subName(name, stream string) string {

	prefix := fmt.Sprintf("%s.", stream)
	name = strings.Replace(name, ".", "_", -1)

	// prefix := ""
	// if stream != "" {
	// 	prefix = fmt.Sprintf("%s.", stream)
	// }
	// if false == strings.HasPrefix(strings.ToUpper(name), prefix) {
	// 	name = prefix + name
	// }

	return fmt.Sprintf("%s%s", prefix, name)
}

func subConsumer(name, stream string) string {
	name = subName(name, stream)
	name = strings.Replace(name, ".", "_", -1)
	return name
}
