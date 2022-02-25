package main

import "fmt"

func main() {

	lst := make(chan int)

	fmt.Printf("len(lst) = %v\n", len(lst))
}
