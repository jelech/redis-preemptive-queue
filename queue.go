package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/tidwall/gjson"
)

var ctx = context.Background()

type errorFunc func() error
type workerFunc func(jsonStr string) error
type callbackFunc func(jsonStr string, status uint)
type Q struct {
	queueName                     string
	queueGroup                    string
	RedisClient                   *redis.Client
	runningMap                    map[string]*workerFunc
	callbackMap                   map[string]*callbackFunc
	beforeWorkFunc, afterWorkFunc errorFunc
	log                           func(...interface{})
	poolChan                      chan struct{} // 控制并发
	reconnectGap                  time.Duration
	notIncreaseWork               bool
}

//NewQuk 新建一个quk队列
func NewQuk(c *Conf) *Q {
	defaultConf(c)

	if err := c.RedisConn.Ping(ctx).Err(); err != nil {
		panic("redis authrpc ping:" + err.Error())
	}

	fmt.Printf("添加监听队列：%s %s\n", c.RedisConn.String(), filepath.Join(c.QueueGroup, c.QueueName))
	return &Q{
		queueName:       filepath.Join(c.QueueGroup, c.QueueName),
		queueGroup:      c.QueueGroup,
		RedisClient:     c.RedisConn,
		notIncreaseWork: c.NotIncreaseWork,
		log:             c.log,
		poolChan:        make(chan struct{}, c.size),
		runningMap:      make(map[string]*workerFunc),
		callbackMap:     make(map[string]*callbackFunc),
		beforeWorkFunc:  func() error { return nil },
		afterWorkFunc:   func() error { return nil },
		reconnectGap:    time.Second,
	}
}

//RegistryWork 注册一个新的name-work到队列中
func (q *Q) RegistryWork(name string, f workerFunc) {
	q.runningMap[name] = &f
}

//RegistryWorks 注册多个
func (q *Q) RegistryWorks(qworks []Qwork) {
	for _, w := range qworks {
		q.RegistryWork(w.Name, w.F)
	}
}

//Run 以服务端的方式运行队列，此方法将会阻塞
func (q *Q) Run() (e error) {
	fmt.Printf("开始监听: %v %v\n", q.RedisClient.String(), q.queueName)
	for {
		q.attentionInfoLog()

		q.poolChan <- struct{}{}

		var jobFrom = q.queueName

		jobs := q.blpopJob(q.queueName)

		var job string
		if len(jobs) == 0 {
			jobFrom, job = q.getJobFromGroupMember()
		} else {
			job = jobs[1]
		}

		// 没有元素, 一般来说不应存在这种事,因为使用了阻塞
		if job != "" {
			q.exploreJob(job)
			q.decrWorkCount(jobFrom)
		}

		<-q.poolChan
		time.Sleep(time.Millisecond) // 休眠1微秒
	}
}

//exploreJob 分发和运行任务工作
func (q *Q) exploreJob(job string) {

	jobName := gjson.Get(job, "name").String()
	if jobName == "" { // 此处应该代码层避免
		q.crashedJobBeforeRun(job, "empty job name got")
		return
	}

	v, ok := q.runningMap[jobName]
	if !ok {
		q.crashedJobBeforeRun(job, "[", jobName, "] is not registered")
		return
	}

	if err := q.beforeWorkFunc(); err != nil {
		q.crashedJobBeforeRun(job, "error before running job")
		return
	}

	q.routine(jobName, gjson.Get(job, "args").String(), v)

	if err := q.afterWorkFunc(); err != nil {
		q.log("running after work function: ", err)
		return
	}
}

//routine  以安全的方式运行一个work
func (q *Q) routine(name string, args string, v *workerFunc) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("routine recover:", v)
			//q.setJobFailed(name, r) // todo 处理任务失败
		}
	}()

	fmt.Printf(""+
		"============================================================================\n"+
		"= [next job: %v]: %v\n"+
		"============================================================================\n", name, args)
	if err := (*v)(args); err != nil {
		//q.setJobFailed(name, err) // todo 处理任务失败
		fmt.Printf(""+
			"============================================================================\n"+
			"= [job: %v] Finished with error: %v\n"+
			"============================================================================\n", err, name)
		return
	}
	fmt.Printf(""+
		"============================================================================\n"+
		"= [job: %v] Finished\n"+
		"============================================================================\n", name)
}

//crashQueueName 获取未成功启动的任务队列名称
func (q *Q) crashQueueName() string {
	return q.queueName + "_crashed_job"
}

//jobCallbackQueueName 获取任务回调队列任务名称
func (q *Q) jobCallbackQueueName() string {
	return q.queueName + "_job_callback"
}

//Submit 提交一个任务到队列中
//将编辑信息到redis消息结构：{"name":"x", "args":itf_into_json}
func (q *Q) Submit(name string, itf interface{}, cbF callbackFunc) error {
	if name == "" {
		return fmt.Errorf("empty name given")
	}

	q.callbackMap[name] = &cbF // todo: 任务回调接口
	var i = struct {
		Name string      `json:"name"`
		Args interface{} `json:"args"`
	}{
		Name: name,
		Args: itf,
	}

	jobInfo, err := json.Marshal(i)
	if err != nil {
		return err
	}

	if err = q.RedisClient.RPush(ctx, q.queueName, string(jobInfo)).Err(); err != nil {
		return err
	}

	if !q.notIncreaseWork {
		q.incrWorkCount()
	}
	return nil
}

func (q *Q) Remove(name, pattern string, val string) (err error) {
	var keys, k []string
	for cursor := uint64(0); ; {
		k, cursor, err = q.RedisClient.Scan(ctx, cursor, q.queueGroup+"*", 1024).Result()
		if err != nil {
			return err
		}

		for _, s := range k {
			if !strings.Contains(s, "workerCountMap") {
				keys = append(keys, s)
			}
		}
		if cursor == 0 {
			break
		}
	}

	var result []*redis.StringSliceCmd
	_, _ = q.RedisClient.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, key := range keys {
			result = append(result, p.LRange(ctx, key, 0, -1))
		}
		return nil
	})

	_, err = q.RedisClient.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, cmd := range result {
			if len(cmd.Args()) <= 1 {
				continue
			}

			key, ok := cmd.Args()[1].(string)
			if !ok {
				continue
			}

			for _, re := range cmd.Val() {
				if gjson.Get(re, "name").String() != name {
					continue
				}

				if val == fmt.Sprint(gjson.Get(re, "args."+pattern).Value()) {
					p.LRem(ctx, key, 1, []byte(re))
				}
			}
		}

		return nil
	})

	return err
}

func (q *Q) BeforeWork(f errorFunc) {
	q.beforeWorkFunc = f
}

func (q *Q) AfterWork(f errorFunc) {
	q.afterWorkFunc = f
}

func (q *Q) crashedJobBeforeRun(job string, errInfo ...interface{}) {
	<-q.poolChan // 释放当前任务空间
	q.log(errInfo)
	//q.RedisClient.LPush(ctx, q.crashQueueName(), job) // todo: 加入crash信息
	//q.RedisClient.Set // todo: set fail reason
}

func (q *Q) setJobFailed(name string, r interface{}) {
	q.RedisClient.LPush(ctx, q.jobCallbackQueueName(), q.workInfo(name, workFailure, r))
}

func (q *Q) setJobSuccess(name string) {
	q.RedisClient.LPush(ctx, q.jobCallbackQueueName(), q.workInfo(name, workSuccess))
}

func (q *Q) attentionInfoLog() {
	crashedJobLen, _ := q.RedisClient.LLen(ctx, q.crashQueueName()).Result()
	if crashedJobLen != 0 {
		fmt.Printf("【请注意】\n")
		fmt.Printf("队列中有 [%d] 个任务处于启动失败队列\n", crashedJobLen)
	}
}

func (q *Q) waitingReconnect() {
	for e := q.RedisClient.Ping(ctx).Err(); e != nil; {
		fmt.Printf("redis链接失败，%ds后等待重连", q.reconnectGap)
		<-time.Tick(q.reconnectGap)
		q.reconnectGap *= 2
	}
}

func (q *Q) workerCountKeyName() string {
	return fmt.Sprintf("%s/workerCountMap", q.queueGroup)
}

func (q *Q) getJobFromGroupMember() (jobFrom, job string) {
	if q.queueGroup == DefaultGroup || strings.Contains(q.queueName, DefaultName) {
		return // 未分组的情况下，不抢占
	}

	res, err := q.RedisClient.HGetAll(ctx, q.workerCountKeyName()).Result()
	if err != nil {
		q.log("get registered worker failed")
		return
	}

	var maxValKey string
	var maxVal int
	for key, val := range res {
		n, _ := strconv.Atoi(val)
		if maxVal < n {
			maxVal = n
			maxValKey = key
		}
	}

	// 没有找到
	if maxVal == 0 {
		return
	}

	jobs := q.blpopJob(maxValKey)
	if len(jobs) != 0 {
		return maxValKey, jobs[1]
	}

	return
}

func (q *Q) incrWorkCount() {
	err := q.RedisClient.HIncrBy(ctx, q.workerCountKeyName(), q.queueName, 1).Err()
	if err != nil {
		q.log("HIncrBy worker failed")
		return
	}
}

func (q *Q) decrWorkCount(jobFrom string) {
	err := q.RedisClient.HIncrBy(ctx, q.workerCountKeyName(), jobFrom, -1).Err()
	if err != nil {
		q.log("HDecrBy worker failed")
		return
	}
}

func (q *Q) blpopJob(key string) []string {
	jobs, err := q.RedisClient.BLPop(ctx, time.Second, key).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		q.log(fmt.Errorf("redis blpop: %v", err))
		q.waitingReconnect()
		return nil
	}
	return jobs
}
