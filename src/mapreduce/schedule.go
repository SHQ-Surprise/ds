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

//golang经验：从没有值的open channel读数据，会阻塞。但是从没有值close的channel读数据，
//不会阻塞，而是读出空值，此时需要用 v,ok := <- channel， 若ok==false则已经close了
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

	m := &sync.Mutex{}
	var wg sync.WaitGroup
	msgs := make(chan msg, ntasks)
	var complete int64

	//input tasks
	wg.Add(1)
	if phase == mapPhase {
		go func() {
			defer wg.Done()
			for i, file := range mapFiles {
				info := msg{file, i}
				msgs <- info
			}
		}()
	} else {
		go func() {
			defer wg.Done()
			for i := 0; i < ntasks; i++ {
				info := msg{index: i}
				msgs <- info
			}
		}()

	}

	idle := make(chan int, ntasks)
	signals := make([]chan msg, 0)
	done := make(chan bool, 1)

	//schedule
	wg.Add(1)
	go func() {
		wg.Done()
		for {
			select {
			case <-done:
				return
			case msg, good := <-msgs:
				if good == false {
					return
				}
			Loop:
				for {
					select {
					case <-done:
						return
					case id, ok := <-idle:
						if ok == false {
							return
						}
						signals[id] <- msg
						break Loop
					}
				}
			}
		}
	}()

	counter := 0
Loop:
	for {
		select {
		case <-done:
			break Loop
		case workerAddr, good := <-registerChan:
			if good == false {
				break Loop
			}
			signals = append(signals, make(chan msg, 1))
			wg.Add(1)
			go func() {
				defer wg.Done()
				addr := workerAddr
				m.Lock()
				id := counter
				counter++
				m.Unlock()
				idle <- id

				for {
					select {
					case <-done:
						return
					case msg, ok := <-signals[id]:
						if ok == false {
							return
						}
						if call(addr, "Worker.DoTask", DoTaskArgs{jobName, msg.file, phase, msg.index, n_other}, nil) == true {
							idle <- id
							atomic.AddInt64(&complete, 1)
							if int(complete) == ntasks {
								close(done)
							}
						} else {
							msgs <- msg
							idle <- id
						}

					}
				}

			}()
		}
	}

	wg.Wait()
	close(msgs)
	close(idle)
	for _, signal := range signals {
		close(signal)
	}

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}

type msg struct {
	file  string
	index int
}
