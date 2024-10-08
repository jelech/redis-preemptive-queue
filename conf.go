package queue

import (
	"github.com/go-redis/redis/v8"
	"log"
)

type Conf struct {
	QueueName  string
	QueueGroup string

	NotIncreaseWork bool

	queueName string // 实际队列 group/name
	RedisURL  string
	RedisConn *redis.Client
	size      uint
	log       func(...interface{})
}

const (
	DefaultGroup = "DEFAULT_QUK_GROUP"
	DefaultName  = "DEFAULT_QUK_NAME"
)

func defaultConf(c *Conf) {
	if c.RedisConn == nil {
		opt, err := redis.ParseURL(c.RedisURL)
		if err != nil {
			panic(err)
		}

		opt.MaxRetries = 5
		opt.PoolSize = 100
		c.RedisConn = redis.NewClient(opt)
	}

	if c.QueueName == "" {
		c.QueueName = DefaultName
	}

	if c.QueueGroup == "" {
		c.QueueGroup = DefaultGroup
	}

	if c.size == 0 {
		c.size = 1
	}

	if c.log == nil {
		c.log = func(v ...interface{}) { log.Print(v) }
	}
}
