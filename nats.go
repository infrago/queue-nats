package queue_nats

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/infrago/queue"
	"github.com/nats-io/nats.go"
)

type (
	natsDriver  struct{}
	natsConnect struct {
		mutex sync.RWMutex

		running bool
		actives int64

		instance *queue.Instance
		setting  natsSetting

		client *nats.Conn
		stream nats.JetStreamContext

		queues []queue.Info
		subs   []*nats.Subscription
	}
	//配置文件
	natsSetting struct {
		Stream   string
		Url      string
		Username string
		Password string
	}
	natsMsg struct {
		Attempt int    `json:"t"`
		After   int64  `json:"a"`
		Data    []byte `json:"d"`
	}
)

// 连接
func (driver *natsDriver) Connect(inst *queue.Instance) (queue.Connect, error) {
	//获取配置信息
	setting := natsSetting{
		Url: nats.DefaultURL,
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

	return &natsConnect{
		instance: inst, setting: setting,
		queues: make([]queue.Info, 0),
		subs:   make([]*nats.Subscription, 0),
	}, nil
}

// 打开连接
func (this *natsConnect) Open() error {
	opts := []nats.Option{}
	if this.setting.Username != "" && this.setting.Password != "" {
		opts = append(opts, nats.UserInfo(this.setting.Username, this.setting.Password))
	}
	client, err := nats.Connect(this.setting.Url, opts...)
	if err != nil {
		return err
	}

	this.client = client

	return nil
}

func (this *natsConnect) Health() (queue.Health, error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return queue.Health{Workload: this.actives}, nil
}

// 关闭连接
func (this *natsConnect) Close() error {
	if this.client != nil {
		this.client.Close()
	}

	return nil
}

func (this *natsConnect) Register(info queue.Info) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.queues = append(this.queues, info)

	return nil
}

// 开始订阅者
func (this *natsConnect) Start() error {
	if this.running {
		return errAlreadyRunning
	}

	nc := this.client

	for _, info := range this.queues {
		// localInfo := info

		//订阅前置，不能在子线程里订阅
		name := info.Name
		sub, err := nc.QueueSubscribeSync(name, name)
		if err != nil {
			return err
		}
		this.subs = append(this.subs, sub)

		//开启循环
		this.instance.Submit(func() {
			for {
				msg, err := sub.NextMsg(time.Second)
				if err != nil {
					//超时就继续
					if err == nats.ErrTimeout {
						continue
					} else {
						break
					}
				}

				after := int64(0)
				if vv := msg.Header.Get("after"); vv != "" {
					if num, err := strconv.ParseInt(vv, 10, 64); err == nil {
						after = num
					}
				}

				//没到处理时间，延迟1秒写回队列
				now := time.Now()
				if after > 0 && after > now.Unix() {
					time.Sleep(time.Second)
					this.publish(msg)
					fmt.Println("pub", msg.Subject)
					continue //跳过
				}

				req := queue.Request{
					msg.Subject, msg.Data, 1, time.Now(),
				}
				//从header里面读attempt
				if vv := msg.Header.Get("attempt"); vv != "" {
					if num, err := strconv.Atoi(vv); err == nil {
						req.Attempt = num
					}
				}

				//正常读到消息，处理
				res := this.instance.Serve(req)
				if res.Retry {
					if msg.Header == nil {
						msg.Header = nats.Header{}
					}

					after := fmt.Sprintf("%v", now.Add(res.Delay).Unix())
					attempt := fmt.Sprintf("%v", req.Attempt+1)

					msg.Header.Set("after", after)
					msg.Header.Set("attempt", attempt)

					this.publish(msg)
				}
			}

		})
	}

	this.running = true
	return nil
}

// 停止订阅
func (this *natsConnect) Stop() error {
	if false == this.running {
		return errNotRunning
	}

	//关闭订阅
	for _, sub := range this.subs {
		sub.Unsubscribe()
	}

	this.running = false
	return nil
}

// enqueue 实际发送方法
func (this *natsConnect) publish(msg *nats.Msg) error {
	if this.client == nil {
		return errInvalidConnection
	}
	err := this.client.PublishMsg(msg)

	if err != nil {
		return err
	}

	return err
}

func (this *natsConnect) Publish(name string, data []byte) error {
	msg := nats.NewMsg(name)
	msg.Data = data

	msg.Header = nats.Header{}
	msg.Header.Add("attempt", "1")

	return this.publish(msg)
}

// DeferredPublish
// 此方法不可靠，有丢消息的可能
func (this *natsConnect) DeferredPublish(name string, data []byte, delay time.Duration) error {
	msg := nats.NewMsg(name)
	msg.Data = data

	after := time.Now().Add(delay).Unix()
	msg.Header = nats.Header{}
	msg.Header.Add("after", fmt.Sprintf("%d", after))
	msg.Header.Add("attempt", "1")

	return this.publish(msg)
}
