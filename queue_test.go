package queue

import (
	"fmt"
	"testing"
)

/*
This is test code
*/

func TestQuk(t *testing.T) {
	l := fmt.Sprintf("redis://:%s@%s/%d", "", "localhost", 0)
	var err error
	q := NewQuk(&Conf{
		QueueName: "abc",
		RedisURL:  l,
	})

	q.BeforeWork(func() error { return nil })
	q.AfterWork(func() error { return nil })

	q.RegistryWork("A", runningFun)
	q.RegistryWorks([]Qwork{
		{"B", runningFun},
		{"C", runningFun},
	})

	var jsonItf struct {
		Name string `json:"name"`
		Sub  struct {
			ID uint `json:"id"`
		} `json:"sub"`
	}

	q.Submit("", jsonItf, callBackFunc)
	q.Submit("", jsonItf, nil)

	err = q.Run()
	fmt.Println(err)
}

func callBackFunc(jsonStr string, statusCode uint) {}

func runningFun(jsonStr string) error {
	return nil
}
