package mr

import (
	"errors"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	mutex sync.Mutex

	files       []string
	nReduce     int
	reducePhase bool

	// MapTask
	mapIndex int

	// ReduceTask
	reduceIndex int

	// Common
	failedWorkers map[int]bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) NReduce(_, reply *NReduceReply) error {
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) RequestTask(_, reply *TaskReply) error {
	reply.ReducePhase = c.reducePhase

	// if c.reducePhase && c.reduceIndex == c.nReduce && len(c.reduceFailedTasks) == 0 {
	if c.Done() {
		reply.Done = true
	} else if !c.reducePhase {
		// Map
		if c.mapIndex > len(c.files) {
			// Only happen with race condition
			return errors.New("mapIndex out of range")
		} else if c.mapIndex == len(c.files) {
			// Check are there running workers
			if len(c.failedWorkers) == 0 {
				// Finish map phase
				c.reducePhase = true
				return c.RequestTask(nil, reply)
			} else {
				// Check are there failed workers
				taskIndex := -1
				for i, v := range c.failedWorkers {
					if v {
						taskIndex = i
						break
					}
				}

				if taskIndex == -1 {
					// All workers running
					// Ask to wait
					reply.Waiting = true
					return nil
				}

				// log.Println("Reassign Map task", taskIndex)
				reply.MapFile = c.files[taskIndex]
				c.failedWorkers[taskIndex] = false

				go c.workerGuard(taskIndex)
			}
		} else {
			// log.Println("Map task", c.mapIndex)
			reply.MapFile = c.files[c.mapIndex]
			c.failedWorkers[c.mapIndex] = false

			go c.workerGuard(c.mapIndex)

			c.mapIndex++
		}
	} else {
		// Reduce
		if c.reduceIndex > c.nReduce {
			return errors.New("reduceIndex out of range")
		} else if c.reduceIndex == c.nReduce {
			// Check are there running workers
			if len(c.failedWorkers) == 0 {
				// Finish reduce phase
				return c.RequestTask(nil, reply)
			} else {
				// Check are there failed workers
				taskIndex := -1
				for i, v := range c.failedWorkers {
					if v {
						taskIndex = i
						break
					}
				}

				if taskIndex == -1 {
					// All workers running
					// Ask to wait
					reply.Waiting = true
					return nil
				}

				// log.Println("Reassign Reduce task", taskIndex)
				reply.ReduceTaskNum = taskIndex
				c.failedWorkers[taskIndex] = false

				go c.workerGuard(taskIndex)
			}
		} else {
			// log.Println("Reduce task", c.reduceIndex)
			reply.ReduceTaskNum = c.reduceIndex
			c.failedWorkers[c.reduceIndex] = false

			go c.workerGuard(c.reduceIndex)

			c.reduceIndex++
		}
	}

	return nil
}

func (c *Coordinator) MapSuccess(args, _ *MapSuccessArgs) error {
	taskIndex := -1
	for i := range c.files {
		if c.files[i] == args.MapFile {
			taskIndex = i
			break
		}
	}

	if _, ok := c.failedWorkers[taskIndex]; ok {
		delete(c.failedWorkers, taskIndex)
	}

	return nil
}

func (c *Coordinator) ReduceSuccess(args, _ *ReduceSuccessArgs) error {
	if _, ok := c.failedWorkers[args.ReduceTaskNum]; ok {
		delete(c.failedWorkers, args.ReduceTaskNum)
	}

	return nil
}

func (c *Coordinator) workerGuard(id int) {
	time.Sleep(10 * time.Second)
	if _, exist := c.failedWorkers[id]; exist {
		// log.Println("Worker", id, "failed")
		c.failedWorkers[id] = true
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// go http.Serve(l, nil)
	go http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		http.DefaultServeMux.ServeHTTP(w, r)
	}))
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.reducePhase && c.reduceIndex == c.nReduce && len(c.failedWorkers) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mutex: sync.Mutex{},

		files:   files,
		nReduce: nReduce,

		mapIndex:    0,
		reduceIndex: 0,

		failedWorkers: make(map[int]bool),
	}

	c.server()
	return &c
}
