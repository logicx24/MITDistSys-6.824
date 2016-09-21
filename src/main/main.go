package main

import (
	"fmt"
	"math"
	"time"
	"strconv"
	"sync"
	"math/rand"
	"encoding/base64"
	crand "crypto/rand"
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

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

type Mutatable struct {
	a int
	b int
	ints []int
}

func (m Mutatable) StayTheSame() {
	m.a = 5
	m.b = 7
	m.ints[0]=2
}

func (m *Mutatable) Mutate() {
	m.a = 5
	m.b = 7
	m.ints[0]=2
}

func main() {
	ma := make(map[int]string)
	ma[1] = "1"
	ma[2] = "2"
	fmt.Println(ma)

	delete(ma, 1)
	fmt.Println(ma)
}


func randInt(min int, max int) int {

	return min + rand.Intn(max-min)
}