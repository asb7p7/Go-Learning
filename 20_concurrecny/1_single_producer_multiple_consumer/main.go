package main

import (
	"fmt"
	"sync"
)

func main() {

	nw := 5
	cs := 10
	chanl := make(chan string, cs)
	wg := sync.WaitGroup{}

	var nm int
	fmt.Println("Enter number of messages: ")
	fmt.Scan(&nm)
	// 	important to remeber
	//  -> wg should be a pointer so that all go routines access
	//     same wg
	//  -> wg.Add() ,  wg.Done() , wg.Wait()
	//  -> initate the consumer thread so that we never go to the      possition of cahnnel full deadlock
	for i := range nw {
		wg.Add(1)
		go run_worker(i, chanl, &wg)
	}

	// 	-> no of workers = no og go rotuines
	// 	-> 1 worker = 1 go routine and write and read both
	//   	 we create new thread

	go addmsg(nm, chanl)
	// 	we have to wait after the channel cloased.
	wg.Wait()

	fmt.Println(" task done ")
}

func run_worker(wid int, chanl <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	for msg := range chanl {
		fmt.Printf(" %v consumed msg :  %v \n ", wid, msg)
	}

}

func addmsg(nm int, chanl chan<- string) {
	//  cloasing channel is vvvvip
	//  if not the program gets blocker here..
	defer close(chanl)
	for i := range nm {
		select {
		case chanl <- fmt.Sprintf(" msgId : %v ", i):
		default:
			fmt.Println(" no space in channel ")
		}
	}
}
