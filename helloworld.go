package main

import (
	"log"
	"math/rand"
	"time"

	"github.com/riaken/riaken-core"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type keyValue [2]string

func main() {
	// Riak cluster addresses
	addrs := []string{"127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086", "127.0.0.1:8087"}
	// Create a client, passing the addresses, and number of connections to
	// maintain per cluster node
	client := riaken_core.NewClient(addrs, 1)
	// Dial the servers
	if err := client.Dial(); err != nil {
		log.Fatal(err.Error()) // all nodes are down
	}
	// Gracefully close all the connections when finished
	defer client.Close()

	// Grab a session to interact with the cluster
	session := client.Session()
	// Release the session
	defer session.Release()

	pool := make(chan keyValue, 10)
	go storeKeys(session, pool)
	readKeys(session, pool)
}

func storeKeys(s *riaken_core.Session, pool chan keyValue) {
	for {
		var (
			key    = randomKey(20)
			value  = randomKey(5)
			bucket = s.GetBucket("pool")
			object = bucket.Object(key)
		)
		if _, err := object.Store([]byte(value)); err != nil {
			log.Print(err.Error())
		}
		log.Printf("stored %s: %s", key, value)
		pool <- keyValue{key, value}
		time.Sleep(10 * time.Millisecond)
	}
}

func readKeys(s *riaken_core.Session, pool chan keyValue) {
	bucket := s.GetBucket("pool")
	var p []keyValue
	for {
		select {
		case e := <-pool:
			p = append(p, e)
		case <-time.After(10 * time.Millisecond):
			e := p[rand.Intn(len(p))]
			key := e[0]
			object := bucket.Object(key)
			data, err := object.Fetch()
			if err != nil {
				log.Print(err.Error())
				continue
			}
			have := string(data.GetContent()[0].GetValue())
			if want := e[1]; have != want {
				log.Print("retrieved have: %s want: %s", have, want)
				continue
			}
			log.Printf("read %s: %s", key, have)
		}
	}
}

func randomKey(l int) string {
	pool := "abcdefghijklmnopqrstuvwyzABCDEFGHIJKLMNOPQRSTUVWYZ"
	res := ""
	for i := 0; i < l; i++ {
		res += string(pool[rand.Intn(len(pool))])
	}
	return res
}
