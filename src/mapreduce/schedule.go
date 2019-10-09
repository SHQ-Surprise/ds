package mapreduce

import (
	"fmt"
	"sync"
	"sync/atomic"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	var complete int64
	var picked int64
	m := &sync.Mutex{}
	var wg sync.WaitGroup
	files := make(chan string, len(mapFiles))
	indexs := make(chan int, ntasks)
	if phase == mapPhase {
		// go func() {
		// 	for i, file := range mapFiles {
		// 		files <- file
		// 		indexs <- i
		// 	}
		// }()
		for i, file := range mapFiles {
			files <- file
			indexs <- i
		}
	} else {
		// go func() {
		// 	for i := 0; i < ntasks; i++ {
		// 		indexs <- i
		// 	}
		// }()
		for i := 0; i < ntasks; i++ {
			indexs <- i
		}
	}

	workers := make(chan string, ntasks)

	for {
		if int(complete) == ntasks {
			break
		}
		if int(picked) == ntasks {
			continue
		}
		select {
		case workerAddr := <-registerChan:
			wg.Add(1)
			go func() {
				defer wg.Done()
				if phase == mapPhase {
					m.Lock()
					file := <-files
					index := <-indexs
					m.Unlock()
					atomic.AddInt64(&picked, 1)
					if call(workerAddr, "Worker.DoTask", DoTaskArgs{jobName, file, phase, index, n_other}, nil) == true {
						atomic.AddInt64(&complete, 1)
					} else {
						m.Lock()
						files <- file
						indexs <- index
						m.Unlock()
						atomic.AddInt64(&picked, -1)
					}
				} else {
					index := <-indexs
					atomic.AddInt64(&picked, 1)
					if call(workerAddr, "Worker.DoTask", DoTaskArgs{jobName, "", phase, index, n_other}, nil) == true {
						atomic.AddInt64(&complete, 1)
					} else {
						indexs <- index
						atomic.AddInt64(&picked, -1)
					}
				}
				workers <- workerAddr
			}()

		case workerAddr := <-workers:
			wg.Add(1)
			go func() {
				defer wg.Done()
				if phase == mapPhase {
					m.Lock()
					file := <-files
					index := <-indexs
					m.Unlock()
					atomic.AddInt64(&picked, 1)
					if call(workerAddr, "Worker.DoTask", DoTaskArgs{jobName, file, phase, index, n_other}, nil) == true {
						atomic.AddInt64(&complete, 1)
					} else if int(complete) < ntasks {
						m.Lock()
						files <- file
						indexs <- index
						m.Unlock()
						atomic.AddInt64(&picked, -1)
					}
				} else {
					index := <-indexs
					atomic.AddInt64(&picked, 1)
					if call(workerAddr, "Worker.DoTask", DoTaskArgs{jobName, "", phase, index, n_other}, nil) == true {
						atomic.AddInt64(&complete, 1)
					} else if int(complete) < ntasks {
						indexs <- index
						atomic.AddInt64(&picked, -1)
					}
				}
				workers <- workerAddr
			}()
		}
	}

	close(files)
	close(indexs)
	wg.Wait()

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
