# queue-nats

`queue-nats` 是 `queue` 模块的 `nats` 驱动。

## 安装

```bash
go get github.com/infrago/queue@latest
go get github.com/infrago/queue-nats@latest
```

## 接入

```go
import (
    _ "github.com/infrago/queue"
    _ "github.com/infrago/queue-nats"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[queue]
driver = "nats"
```

## 公开 API（摘自源码）

- `func (d *natsDriver) Connect(inst *queue.Instance) (queue.Connection, error)`
- `func (c *natsConnection) Open() error`
- `func (c *natsConnection) Close() error`
- `func (c *natsConnection) Register(name string) error`
- `func (c *natsConnection) Start() error`
- `func (c *natsConnection) Stop() error`
- `func (c *natsConnection) Publish(name string, data []byte) error`
- `func (c *natsConnection) DeferredPublish(name string, data []byte, delay time.Duration) error`
- `func (d *natsJSDriver) Connect(inst *queue.Instance) (queue.Connection, error)`
- `func (c *natsJSConnection) Open() error`
- `func (c *natsJSConnection) Close() error`
- `func (c *natsJSConnection) Register(name string) error`
- `func (c *natsJSConnection) Start() error`
- `func (c *natsJSConnection) Stop() error`
- `func (c *natsJSConnection) Publish(name string, data []byte) error`
- `func (c *natsJSConnection) DeferredPublish(name string, data []byte, delay time.Duration) error`

## 排错

- driver 未生效：确认模块段 `driver` 值与驱动名一致
- 连接失败：检查 endpoint/host/port/鉴权配置
