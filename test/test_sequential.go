package main

import (
	"dbService/utils"
	"fmt"
	"log"
	"net/rpc"
	"strconv"
)

// runSequentialTest esegue test per verificare il rispetto della consistenza sequenziale
func runSequentialTest() {
	// Crea i client, ognuno dei quali interagisce con una diversa replica del datastore
	var clients []*Client
	for i := 0; i < NumReplicas; i++ {
		// Il client si collega al server RPC
		rpcClient, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(BasePortToClient+i))
		if err != nil {
			log.Fatal("Error in dialing: ", err)
		}

		defer func(client *rpc.Client) {
			err := client.Close()
			if err != nil {
				log.Fatal("Error while closing connection:", err)
			}
		}(rpcClient)

		fmt.Printf("Client %d connesso al server %d\n", i, BasePortToClient+i)
		client := &Client{
			ID:        i,
			rpcClient: rpcClient,
			requests:  nil,
		}

		//aggiunge il client alla lista dei client
		clients = append(clients, client)
	}

	// Costruisce la lista di richieste che ogni client deve realizzare verso la rispettiva replica.
	// Le operazioni da svolgere non sono casuali, ma sono definite a priori così da poter verificare il perseguimento della consistenza voluta
	// CLIENT 0
	requests := []Request{
		{op: utils.PUT, key: "x", value: "a"},
		{op: utils.DELETE, key: "x"},
		{op: utils.GET, key: "z"},
		{op: utils.GET, key: "x"},
	}

	clients[0].requests = requests

	// CLIENT 1
	requests = []Request{
		{op: utils.PUT, key: "x", value: "b"},
		{op: utils.GET, key: "x"},
		{op: utils.PUT, key: "z", value: "b"},
		{op: utils.GET, key: "z"},
	}

	clients[1].requests = requests

	// CLIENT 2
	requests = []Request{
		{op: utils.GET, key: "x"},
		{op: utils.GET, key: "x"},
		{op: utils.PUT, key: "z", value: "c"},
		{op: utils.GET, key: "z"},
	}

	clients[2].requests = requests

	// I client vengono lanciati in parallelo, ciascuno su una diversa goroutine, e vengono eseguite le rispettive richieste verso il db
	for i := 0; i < NumReplicas; i++ {
		go clients[i].LaunchRequest()
	}
	select {}
}
