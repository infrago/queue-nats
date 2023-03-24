package queue_nats

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/infrago/infra"
	"github.com/infrago/queue"
)

type (
	natsjsDriver  struct{}
	natsjsConnect struct {
		mutex sync.RWMutex

		running bool
		actives int64

		instance *queue.Instance
		setting  natsjsSetting

		client *nats.Conn
		stream nats.JetStreamContext

		queues   []queue.Info
		subs     []*nats.Subscription
		exitChan chan struct{}
	}
	//配置文件
	natsjsSetting struct {
		Stream   string
		Url      string
		Username string
		Password string
	}
)

// 连接
func (driver *natsjsDriver) Connect(inst *queue.Instance) (queue.Connect, error) {
	//获取配置信息
	setting := natsjsSetting{
		Stream: infra.INFRA,
		Url:    nats.DefaultURL,
	}

	if name := infra.Name(); name != "" {
		setting.Stream = strings.ToUpper(name)
	}

	if inst.Config.Prefix != "" {
		setting.Stream = strings.ToUpper(inst.Config.Prefix)
	}

	if vv, ok := inst.Config.Setting["url"].(string); ok {
		setting.Url = vv
	}
	if vv, ok := inst.Config.Setting["stream"].(string); ok {
		setting.Stream = strings.ToUpper(vv)
	}

	if vv, ok := inst.Config.Setting["user"].(string); ok {
		setting.Username = vv
	}
	if vv, ok := inst.Config.Setting["username"].(string); ok {
		setting.Username = vv
	}
	if vv, ok := inst.Config.Setting["pass"].(string); ok {
		setting.Password = vv
	}
	if vv, ok := inst.Config.Setting["password"].(string); ok {
		setting.Password = vv
	}

	//for queue
	setting.Stream += "Q"

	return &natsjsConnect{
		instance: inst, setting: setting,
		queues: make([]queue.Info, 0),
		subs:   make([]*nats.Subscription, 0),
	}, nil
}

// 打开连接
func (this *natsjsConnect) Open() error {
	opts := []nats.Option{}
	if this.setting.Username != "" && this.setting.Password != "" {
		opts = append(opts, nats.UserInfo(this.setting.Username, this.setting.Password))
	}
	client, err := nats.Connect(this.setting.Url, opts...)
	if err != nil {
		return err
	}

	this.client = client

	js, err := client.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return err
	}

	//处理stream
	_, err = js.StreamInfo(this.setting.Stream)
	if err != nil {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     this.setting.Stream,
			Subjects: []string{this.setting.Stream + ".*"},
		})

		if err != nil {
			return err
		}
	}

	this.stream = js

	return nil
}

func (this *natsjsConnect) Health() (queue.Health, error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return queue.Health{Workload: this.actives}, nil
}

// 关闭连接
func (this *natsjsConnect) Close() error {
	if this.client != nil {
		this.client.Close()
		this.client = nil
	}

	return nil
}

func (this *natsjsConnect) Register(info queue.Info) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.queues = append(this.queues, info)

	return nil
}

// 开始订阅者
func (this *natsjsConnect) Start() error {
	if this.running {
		return errAlreadyRunning
	}

	js := this.stream

	for i, info := range this.queues {
		localInfo := info

		//订阅要前置，要不然触发器会先执行，这里还没订好

		name := subName(localInfo.Name, this.setting.Stream)
		consumer := subConsumer(localInfo.Name, this.setting.Stream)

		subOpts := []nats.SubOpt{
			nats.Durable(consumer),
			nats.ManualAck(),
			nats.DeliverNew(),
		}

		sub, err := js.QueueSubscribeSync(name, consumer, subOpts...)

		// fmt.Println("xxx", consumer)
		// fmt.Println("qqq", infname, err)

		if err != nil {
			return err
		}

		this.subs = append(this.subs, sub)

		//循环放在多线程里跑
		this.instance.Submit(func() {
			this.loop(i, localInfo.Name, sub)
		})
	}

	this.running = true
	return nil
}

func (this *natsjsConnect) loop(i int, name string, sub *nats.Subscription) {

	for {
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			if err == nats.ErrTimeout {
				continue // //超时就继续
			} else {
				break
			}
		}

		// fmt.Println("queue", name, i)

		req := queue.Request{
			name, msg.Data, 1, time.Now(),
		}

		tmd, err := msg.Metadata()
		if err != nil {
			msg.Nak()
			continue //失败就丢回这条消息，继续
		}

		req.Attempt = int(tmd.NumDelivered)
		req.Timestamp = tmd.Timestamp

		//直接拉回如果是延迟队列
		if vv := msg.Header.Get("delay"); vv != "" {
			if req.Attempt == 1 {
				delay := time.Duration(0)
				if num, err := strconv.ParseInt(vv, 10, 64); err == nil {
					delay = time.Duration(num)
				}
				msg.NakWithDelay(delay)
				continue
			} else {
				req.Attempt -= 1 //真实减1
			}
		}

		//正常读到消息，处理
		res := this.instance.Serve(req)
		if res.Retry {
			msg.NakWithDelay(res.Delay)
		} else {
			msg.Ack()
		}
	}
}

// 停止订阅
func (this *natsjsConnect) Stop() error {
	if false == this.running {
		return errNotRunning
	}

	//关闭管道
	for _, sub := range this.subs {
		sub.Unsubscribe()
	}

	this.running = false
	return nil
}

func (this *natsjsConnect) Publish(name string, data []byte) error {
	if this.client == nil {
		return errInvalidConnection
	}

	js := this.stream

	name = subName(name, this.setting.Stream)

	_, err := js.Publish(name, data)

	if err != nil {
		return err
	}

	return err
}

func (this *natsjsConnect) DeferredPublish(name string, data []byte, delay time.Duration) error {
	if this.client == nil {
		return errInvalidConnection
	}

	js := this.stream

	name = subName(name, this.setting.Stream)

	msg := &nats.Msg{Subject: name, Data: data}
	msg.Header = nats.Header{}
	msg.Header.Add("delay", fmt.Sprintf("%d", delay))

	_, err := js.PublishMsg(msg)

	if err != nil {
		return err
	}

	return err
}
