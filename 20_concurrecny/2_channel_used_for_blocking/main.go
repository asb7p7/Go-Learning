package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	nw := 3
	cs := 10

	chanl := make(chan string, cs)

	var wg sync.WaitGroup

	for i := range nw {
		wg.Add(1)
		go runWorker(i, chanl, &wg)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	addMsg(chanl, sigChan)

	wg.Wait()

	fmt.Println("Task done")
}

func runWorker(
	wid int,
	chanl <-chan string,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for msg := range chanl {
		fmt.Printf("Worker %d consumed: %s\n", wid, msg)
	}

	fmt.Printf("Worker %d exiting\n", wid)
}

func addMsg(
	chanl chan<- string,
	sigChan <-chan os.Signal,
) {
	defer close(chanl)

	inputCh := make(chan string)

	// Dedicated goroutine for terminal input.
	go func() {
		for {
			fmt.Print("Enter message: ")

			var inp string
			fmt.Scan(&inp)

			inputCh <- inp
		}
	}()

	/*
	   Flow:
	   1. Input goroutine blocks on fmt.Scan().
	   2. addMsg blocks on select(), waiting for:
	        - terminal input from inputCh
	        - Ctrl+C from sigChan
	   3. When input arrives, it is forwarded to worker channel.
	   4. When Ctrl+C is pressed, Go runtime sends os.Interrupt to sigChan.
	   5. addMsg returns and closes chanl.
	   6. Workers exit after channel close.
	   7. wg.Wait() unblocks and program exits.

	   Note:
	   The input goroutine may still be blocked in fmt.Scan(),
	   but that's okay because the process is shutting down.
	*/

	for {
		select {
		case <-sigChan:
			fmt.Println("\nShutdown signal received")
			return

		case input := <-inputCh:
			chanl <- fmt.Sprintf("msgId: %s", input)
		}
	}
}
