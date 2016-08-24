package main

import (
	"fmt"
	"math"
	"time"
	"strconv"
	"sync"
	"math/rand"
)

const Threadhold = float64(0.0000001)

func Sqrt(x float64) float64 {
	var zn float64 = x
	var zn1 float64 = zn - (zn*zn-x)/(2*zn)
	for {
		t := math.Abs(zn1-zn)
		fmt.Println(t,zn,zn1)
		if t!=0.0 && t < Threadhold {
			break;
		}

		tmp := zn1
		zn1 = zn - (zn*zn-x)/(2*zn)
		zn = tmp
	}

	return zn1
}

func say(s string) {

	wg.Add(1)
	defer wg.Done()
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		fmt.Println(strconv.Itoa(i)+":"+s)
	}
}

var wg sync.WaitGroup

type a struct {
	b []int
	t string
}

func main() {
	c1 := make(chan string)
	c2 := make(chan string)

	go func() {
		for i := 0; i < 20000000000; i++{
			time.Sleep(time.Millisecond * 100)
			//c1 <- "one"
		}
	}()
	go func() {
		for i := 0; i < 20000000000; i++{
			time.Sleep(time.Millisecond * 110)
			//c2 <- "two"
		}
	}()

	for  {
		select {
		case msg1 := <-c1:
			fmt.Println("1 received", msg1)
		case msg2 := <-c2:
			fmt.Println("2 received", msg2)
		case <-time.After(time.Duration(200) * time.Millisecond):
			fmt.Println("time up")
			//return
		}
	}
}


func randInt(min int, max int) int {

	return min + rand.Intn(max-min)
}