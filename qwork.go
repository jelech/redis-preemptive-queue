package queue

import (
	"encoding/json"
	"fmt"
)

const (
	workProcess = 1
	workSuccess = 2
	workFailure = 3
)

type Qwork struct {
	Name string
	F    workerFunc
}

type workInfo struct {
	name   string
	status uint
	info   string
}

func (q *Q) workInfo(name string, status uint, r ...interface{}) string {
	b, _ := json.Marshal(workInfo{
		name:   name,
		status: status,
		info:   fmt.Sprintln(r),
	})
	return string(b)
}
