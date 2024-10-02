package main

/*import (
	"dbService/utils"
	"fmt"
	"log"
	"net/rpc"
	"strconv"
)

func connectMultiClientsToServer(serverIndex int, clientIndex int) {
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
		Key:   "a" + strconv.Itoa(serverIndex) + strconv.Itoa(clientIndex),
		Value: "b" + strconv.Itoa(serverIndex) + strconv.Itoa(clientIndex),
	}

	var reply utils.Result

	//prima operazione
	err = client.Call("Datastore.Put", args, &reply)
	if err != nil {
		log.Fatal("Error while executing op: ", err)
	}

	//seconda operazione
	args.Key = "b" + strconv.Itoa(serverIndex) + strconv.Itoa(clientIndex)
	args.Value = "c" + strconv.Itoa(serverIndex) + strconv.Itoa(clientIndex)
	err = client.Call("Datastore.Put", args, &reply)
	if err != nil {
		log.Fatal("Error while executing op: ", err)
	}

	//terza operazione
	args.Key = "b" + strconv.Itoa(serverIndex) + strconv.Itoa(clientIndex)
	err = client.Call("Datastore.Get", args, &reply)
	if err != nil {
		log.Fatal("Error while executing op: ", err)
	}

	//quarta operazione
	args.Key = "a" + strconv.Itoa(serverIndex) + strconv.Itoa(clientIndex)
	err = client.Call("Datastore.Delete", args, &reply)
	if err != nil {
		log.Fatal("Error while executing op: ", err)
	}

	//quinta operazione
	args.Key = "c" + strconv.Itoa(serverIndex) + strconv.Itoa(clientIndex)
	args.Value = "d" + strconv.Itoa(serverIndex) + strconv.Itoa(clientIndex)
	err = client.Call("Datastore.Put", args, &reply)
	if err != nil {
		log.Fatal("Error while executing op: ", err)
	}

	//sesta operazione
	args.Key = "c" + strconv.Itoa(serverIndex) + strconv.Itoa(clientIndex)
	err = client.Call("Datastore.Get", args, &reply)
	if err != nil {
		log.Fatal("Error while executing op: ", err)
	}

}

/*func main() {
	clientIndex := 0
	for i := 0; i < NumClients; i++ {
		go connectMultiClientsToServer(i, clientIndex)
		clientIndex++
		go connectMultiClientsToServer(i, clientIndex)
		clientIndex++
	}
	select {}
}*/
