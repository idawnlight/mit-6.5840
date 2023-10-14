package mr

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	var nReduceReply NReduceReply
	if ok := call("Coordinator.NReduce", &nReduceReply, &nReduceReply); !ok {
		log.Fatal("call NReduce failed")
	}
	nReduce := nReduceReply.NReduce

	taskReply := NewTaskReply()
	if ok := call("Coordinator.RequestTask", &taskReply, &taskReply); !ok {
		log.Fatal("call RequestTask failed")
	}

	if taskReply.Done {
		// log.Println("Worker received done signal")
		return
	} else if taskReply.Waiting {
		// log.Println("Worker received waiting signal")
		time.Sleep(100 * time.Millisecond)
	} else if !taskReply.ReducePhase {
		// Map
		filename := taskReply.MapFile
		cleanFilename := strings.Split(filename, "/")[1]
		cleanFilename = strings.Replace(cleanFilename, ".txt", "", -1)

		// log.Println("Worker received map task", cleanFilename)

		if file, err := os.Open(filename); err != nil {
			log.Fatal("cannot open %v", filename)
		} else {
			defer file.Close()

			var kva []KeyValue

			if content, err := io.ReadAll(file); err != nil {
				log.Fatal("cannot read %v", filename)
			} else {
				kva = mapf(filename, string(content))
			}

			// Create intermediate files
			intermediateFiles := make([]*os.File, nReduce)
			for i := 0; i < nReduce; i++ {
				fileName := fmt.Sprintf("mr-%v-%v", cleanFilename, i)
				if file, err := os.Create(fileName); err != nil {
					log.Fatalf("cannot create %v", cleanFilename)
				} else {
					intermediateFiles[i] = file
				}
			}

			// Write intermediate files
			for _, kv := range kva {
				reduceTaskNum := ihash(kv.Key) % nReduce
				fmt.Fprintf(intermediateFiles[reduceTaskNum], "%v %v\n", kv.Key, kv.Value)
			}

			// Close intermediate files
			for _, file := range intermediateFiles {
				file.Close()
			}

			// Notify coordinator
			// log.Println("Worker finished map task", cleanFilename)
			var mapSuccessArgs MapSuccessArgs
			mapSuccessArgs.MapFile = filename
			if ok := call("Coordinator.MapSuccess", &mapSuccessArgs, &mapSuccessArgs); !ok {
				log.Fatal("call MapSuccess failed")
			}
		}
	} else {
		// Reduce
		taskNum := taskReply.ReduceTaskNum

		// log.Println("Worker received reduce task", taskNum)

		outputFileName := fmt.Sprintf("mr-out-%v", taskNum)

		// Create if not exist
		if _, err := os.Stat(outputFileName); os.IsNotExist(err) {
			if file, err := os.Create(outputFileName); err != nil {
				log.Fatalf("cannot create %v", outputFileName)
			} else {
				file.Close()
			}
		}

		outputFile, err := os.OpenFile(outputFileName, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("cannot create %v", outputFileName)
		}

		kva := make(map[string][]string)

		// Read intermediate files
		filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
			if !strings.HasPrefix(info.Name(), "mr-pg") || !strings.HasSuffix(info.Name(), fmt.Sprintf("-%v", taskNum)) {
				return nil
			}

			// log.Println("Worker received reduce file", path)

			if file, err := os.Open(path); err != nil {
				log.Fatalf("cannot open %v", path)
			} else {
				defer file.Close()

				fileScanner := bufio.NewScanner(file)
				fileScanner.Split(bufio.ScanLines)

				i := 0
				for fileScanner.Scan() {
					line := fileScanner.Text()

					if line == "" {
						continue
					}

					split := strings.Split(line, " ")
					key := split[0]
					value := split[1]

					kva[key] = append(kva[key], value)

					i++
				}
			}
			return nil
		})

		result := make(map[string]string)
		keys := make([]string, 0, len(kva))

		for key := range kva {
			// fmt.Fprintf(outputFile, "%v %v\n", key, reducef(key, kva[key]))
			result[key] = reducef(key, kva[key])
			keys = append(keys, key)
		}

		// Sort keys
		sort.Strings(keys)

		// Write to output file
		for _, key := range keys {
			fmt.Fprintf(outputFile, "%v %v\n", key, result[key])
		}

		// Notify coordinator
		var reduceSuccessArgs ReduceSuccessArgs
		reduceSuccessArgs.ReduceTaskNum = taskNum
		if ok := call("Coordinator.ReduceSuccess", &reduceSuccessArgs, &reduceSuccessArgs); !ok {
			log.Fatal("call ReduceSuccess failed")
		}
	}

	Worker(mapf, reducef)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
