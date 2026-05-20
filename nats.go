package queue_nats

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/infrago/infra"
	"github.com/infrago/queue"
	"github.com/nats-io/nats.go"
)

func init() {
	infra.Register("nats", &natsDriver{})
	js := &natsJSDriver{}
	infra.Register("natsjs", js)
	infra.Register("nats-js", js)
	infra.Register("jetstream", js)
}

type (
	natsDriver struct{}

	natsConnection struct {
		mutex    sync.RWMutex
		running  bool
		instance *queue.Instance
		setting  natsSetting
		client   *nats.Conn
		queues   map[string]int
		subs     []*nats.Subscription
		done     chan struct{}
		wg       sync.WaitGroup
	}

	natsJSDriver struct{}

	natsJSConnection struct {
		mutex    sync.RWMutex
		running  bool
		instance *queue.Instance
		setting  natsSetting
		client   *nats.Conn
		stream   nats.JetStreamContext
		queues   map[string]int
		subs     []*nats.Subscription
		done     chan struct{}
		wg       sync.WaitGroup
	}

	natsSetting struct {
		URL      string
		Token    string
		Username string
		Password string
		Stream   string
	}
)

func parseSetting(inst *queue.Instance) natsSetting {
	cfg := inst.Config.Setting
	setting := natsSetting{
		URL:    nats.DefaultURL,
		Stream: "INFRAGOQ",
	}

	if v, ok := cfg["url"].(string); ok && v != "" {
		setting.URL = v
	}
	if v, ok := cfg["server"].(string); ok && v != "" {
		setting.URL = v
	}
	if v, ok := cfg["token"].(string); ok {
		setting.Token = v
	}
	if v, ok := cfg["user"].(string); ok && v != "" {
		setting.Username = v
	}
	if v, ok := cfg["username"].(string); ok && v != "" {
		setting.Username = v
	}
	if v, ok := cfg["pass"].(string); ok {
		setting.Password = v
	}
	if v, ok := cfg["password"].(string); ok {
		setting.Password = v
	}
	if v, ok := cfg["stream"].(string); ok && v != "" {
		setting.Stream = strings.ToUpper(v)
	}
	return setting
}

func connectNats(setting natsSetting) (*nats.Conn, error) {
	opts := make([]nats.Option, 0)
	if setting.Token != "" {
		opts = append(opts, nats.Token(setting.Token))
	}
	if setting.Username != "" || setting.Password != "" {
		opts = append(opts, nats.UserInfo(setting.Username, setting.Password))
	}
	return nats.Connect(setting.URL, opts...)
}

func (d *natsDriver) Connect(inst *queue.Instance) (queue.Connection, error) {
	return &natsConnection{
		instance: inst,
		setting:  parseSetting(inst),
		queues:   make(map[string]int, 0),
		subs:     make([]*nats.Subscription, 0),
		done:     make(chan struct{}),
	}, nil
}

func (c *natsConnection) Open() error {
	nc, err := connectNats(c.setting)
	if err != nil {
		return err
	}
	c.client = nc
	return nil
}

func (c *natsConnection) Close() error {
	if c.client != nil {
		c.client.Close()
	}
	return nil
}

func (c *natsConnection) Register(name string) error {
	c.mutex.Lock()
	c.queues[name]++
	c.mutex.Unlock()
	return nil
}

func (c *natsConnection) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.running {
		return nil
	}

	for queueName, workers := range c.queues {
		for i := 0; i < workers; i++ {
			name := queueName
			sub, err := c.client.QueueSubscribeSync(name, name)
			if err != nil {
				c.cleanup()
				return err
			}
			c.subs = append(c.subs, sub)

			c.wg.Add(1)
			c.instance.Submit(func() {
				defer c.wg.Done()
				c.consume(name, sub)
			})
		}
	}

	c.running = true
	return nil
}

func (c *natsConnection) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.running {
		return nil
	}
	close(c.done)
	c.stopSubscriptions()
	c.running = false
	return nil
}

func (c *natsConnection) cleanup() {
	close(c.done)
	c.stopSubscriptions()
}

func (c *natsConnection) stopSubscriptions() {
	for _, sub := range c.subs {
		_ = sub.Unsubscribe()
	}
	c.wg.Wait()
	c.subs = nil
	c.done = make(chan struct{})
}

func (c *natsConnection) consume(name string, sub *nats.Subscription) {
	for {
		select {
		case <-c.done:
			return
		default:
		}

		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			logNatsError("read", name, err)
			return
		}

		now := time.Now()
		var after time.Time
		if vv := msg.Header.Get("after"); vv != "" {
			if num, err := strconv.ParseInt(vv, 10, 64); err == nil {
				after = natsAfterTime(num)
			} else {
				logNatsError("parse delay", name, err)
			}
		}
		attempt := 1
		if vv := msg.Header.Get("attempt"); vv != "" {
			if num, err := strconv.Atoi(vv); err == nil && num > 0 {
				attempt = num
			} else {
				logNatsError("parse attempt", name, err)
			}
		}

		if !after.IsZero() && after.After(now) {
			delay := time.Until(after)
			if delay > time.Second {
				delay = time.Second
			}
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-c.done:
				timer.Stop()
				return
			}
			if err := c.publish(msg.Subject, msg.Data, attempt, time.Until(after)); err != nil {
				logNatsError("defer", name, err)
			}
			continue
		}

		req := queue.Request{
			Name:      name,
			Data:      msg.Data,
			Attempt:   attempt,
			Timestamp: now,
		}
		res := c.instance.Serve(req)
		if res.Retry {
			if err := c.publish(name, msg.Data, attempt+1, res.Delay); err != nil {
				logNatsError("retry", name, err)
			}
		}
	}
}

func (c *natsConnection) publish(name string, data []byte, attempt int, delay time.Duration) error {
	msg := nats.NewMsg(name)
	msg.Data = data
	msg.Header = nats.Header{}
	msg.Header.Set("attempt", strconv.Itoa(attempt))
	if delay > 0 {
		msg.Header.Set("after", strconv.FormatInt(time.Now().Add(delay).UnixNano(), 10))
	}
	return c.client.PublishMsg(msg)
}

func (c *natsConnection) Publish(name string, data []byte) error {
	return c.publish(name, data, 1, 0)
}

func (c *natsConnection) DeferredPublish(name string, data []byte, delay time.Duration) error {
	return c.publish(name, data, 1, delay)
}

func (d *natsJSDriver) Connect(inst *queue.Instance) (queue.Connection, error) {
	return &natsJSConnection{
		instance: inst,
		setting:  parseSetting(inst),
		queues:   make(map[string]int, 0),
		subs:     make([]*nats.Subscription, 0),
		done:     make(chan struct{}),
	}, nil
}

func (c *natsJSConnection) Open() error {
	nc, err := connectNats(c.setting)
	if err != nil {
		return err
	}
	c.client = nc

	js, err := nc.JetStream()
	if err != nil {
		return err
	}
	_, err = js.StreamInfo(c.setting.Stream)
	if err != nil {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     c.setting.Stream,
			Subjects: []string{c.setting.Stream + ".*"},
		})
		if err != nil {
			return err
		}
	}
	c.stream = js
	return nil
}

func (c *natsJSConnection) Close() error {
	if c.client != nil {
		c.client.Close()
	}
	return nil
}

func (c *natsJSConnection) Register(name string) error {
	c.mutex.Lock()
	c.queues[name]++
	c.mutex.Unlock()
	return nil
}

func (c *natsJSConnection) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.running {
		return nil
	}

	for queueName, workers := range c.queues {
		for i := 0; i < workers; i++ {
			qName := queueName
			subject := jsSubject(c.setting.Stream, qName)
			consumer := jsConsumer(c.setting.Stream, qName)
			group := jsQueueGroup(c.setting.Stream, qName)

			sub, err := c.stream.QueueSubscribeSync(subject, group,
				nats.Durable(consumer),
				nats.DeliverAll(),
				nats.ManualAck(),
			)
			if err != nil {
				c.cleanup()
				return err
			}
			c.subs = append(c.subs, sub)

			c.wg.Add(1)
			c.instance.Submit(func() {
				defer c.wg.Done()
				c.consume(qName, sub)
			})
		}
	}

	c.running = true
	return nil
}

func (c *natsJSConnection) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.running {
		return nil
	}
	close(c.done)
	c.stopSubscriptions()
	c.running = false
	return nil
}

func (c *natsJSConnection) cleanup() {
	close(c.done)
	c.stopSubscriptions()
}

func (c *natsJSConnection) stopSubscriptions() {
	for _, sub := range c.subs {
		_ = sub.Unsubscribe()
	}
	c.wg.Wait()
	c.subs = nil
	c.done = make(chan struct{})
}

func (c *natsJSConnection) consume(qName string, sub *nats.Subscription) {
	for {
		select {
		case <-c.done:
			return
		default:
		}

		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			logNatsError("read", qName, err)
			return
		}

		req := queue.Request{
			Name:      qName,
			Data:      msg.Data,
			Attempt:   1,
			Timestamp: time.Now(),
		}

		md, err := msg.Metadata()
		if err != nil {
			logNatsError("metadata", qName, err)
			if err := msg.Nak(); err != nil {
				logNatsError("nak", qName, err)
			}
			continue
		}
		req.Attempt = int(md.NumDelivered)
		req.Timestamp = md.Timestamp

		if vv := msg.Header.Get("after"); vv != "" {
			after := time.Time{}
			if num, err := strconv.ParseInt(vv, 10, 64); err == nil {
				after = natsAfterTime(num)
			} else {
				logNatsError("parse delay", qName, err)
			}
			if !after.IsZero() && after.After(time.Now()) {
				if err := msg.NakWithDelay(time.Until(after)); err != nil {
					logNatsError("delay", qName, err)
				}
				continue
			}
			if req.Attempt > 1 {
				req.Attempt--
			}
		} else if vv := msg.Header.Get("delay"); vv != "" {
			delay := time.Duration(0)
			if num, err := strconv.ParseInt(vv, 10, 64); err == nil {
				delay = time.Duration(num)
			} else {
				logNatsError("parse delay", qName, err)
			}
			if req.Attempt == 1 && delay > 0 {
				if err := msg.NakWithDelay(delay); err != nil {
					logNatsError("delay", qName, err)
				}
				continue
			}
			req.Attempt--
		}

		res := c.instance.Serve(req)
		if res.Retry {
			if err := msg.NakWithDelay(res.Delay); err != nil {
				logNatsError("retry", qName, err)
			}
		} else if err := msg.Ack(); err != nil {
			logNatsError("ack", qName, err)
		}
	}
}

func (c *natsJSConnection) Publish(name string, data []byte) error {
	_, err := c.stream.Publish(jsSubject(c.setting.Stream, name), data)
	return err
}

func (c *natsJSConnection) DeferredPublish(name string, data []byte, delay time.Duration) error {
	msg := nats.NewMsg(jsSubject(c.setting.Stream, name))
	msg.Data = data
	msg.Header = nats.Header{}
	msg.Header.Set("after", fmt.Sprintf("%d", time.Now().Add(delay).UnixNano()))
	_, err := c.stream.PublishMsg(msg)
	return err
}

func jsSubject(stream, name string) string {
	name = strings.ReplaceAll(name, ".", "_")
	return fmt.Sprintf("%s.%s", stream, name)
}

func jsConsumer(stream, name string) string {
	name = jsSubject(stream, name)
	return strings.ReplaceAll(name, ".", "_")
}

func jsQueueGroup(stream, name string) string {
	return jsConsumer(stream, name) + "_workers"
}

func natsAfterTime(value int64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	if value < 1_000_000_000_000 {
		return time.Unix(value, 0)
	}
	return time.Unix(0, value)
}

func logNatsError(action, name string, err error) {
	if err != nil {
		fmt.Printf("infrago queue nats %s failed on %s: %v\n", action, name, err)
	}
}

var _ queue.Connection = (*natsConnection)(nil)
var _ queue.Connection = (*natsJSConnection)(nil)
