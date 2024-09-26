package main

import (
	"dbService/utils"
	"fmt"
	"log"
	"net/rpc"
	"strconv"
)

const BasePort = 8080
const NumClients = 3

func connectToServer(serverIndex int) {
	fmt.Printf("Connecting to server: %d\n", BasePort+serverIndex)

	// Connessione al server RPC
	client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(BasePort+serverIndex))
	if err != nil {
		log.Fatal("Error in dialing: ", err)
	}

	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {
			log.Fatal("Error while closing connection:", err)
		}
	}(client)

	args := utils.Args{
		Key:   "a" + strconv.Itoa(serverIndex),
		Value: "b" + strconv.Itoa(serverIndex),
	}

	var reply utils.Result

	//prima operazione
	err = client.Call("Datastore.Put", args, &reply)
	if err != nil {
		log.Fatal("Error while executing op: ", err)
	}

	//seconda operazione
	args.Key = "b" + strconv.Itoa(serverIndex)
	args.Value = "c" + strconv.Itoa(serverIndex)
	err = client.Call("Datastore.Put", args, &reply)
	if err != nil {
		log.Fatal("Error while executing op: ", err)
	}

	//terza operazione
	args.Key = "a" + strconv.Itoa(serverIndex)
	err = client.Call("Datastore.Delete", args, &reply)
	if err != nil {
		log.Fatal("Error while executing op: ", err)
	}

	//quarta operazione
	args.Key = "c" + strconv.Itoa(serverIndex)
	args.Value = "d" + strconv.Itoa(serverIndex)
	err = client.Call("Datastore.Put", args, &reply)
	if err != nil {
		log.Fatal("Error while executing op: ", err)
	}
}

func main() {
	for i := 0; i < NumClients; i++ {
		go connectToServer(i)
	}
	select {}
}
