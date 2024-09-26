package main

import (
	"dbService/utils"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

const NumReplicas = 3
const BasePort = 12345
const BasePortToClient = 8080

func main() {
	// recupera da riga di comando l'indice da assegnare al server lanciato
	if len(os.Args) < 2 {
		fmt.Println("Please provide an index as an argument.")
		return
	}

	// Recupera l'indice passato da riga di comando
	indexStr := os.Args[1]
	serverIndex, err := strconv.Atoi(indexStr)
	if err != nil {
		fmt.Println("Invalid index:", indexStr)
		return
	}

	fmt.Printf("Starting server instance with index: %d\n", serverIndex)

	//Create an instance of struct which implements datastore interface
	dbSequential := &DbSequential{
		ID: serverIndex,
		DbStore: DbStore{
			Store: make(map[string]string),
			mutex: sync.Mutex{},
		},
		MessageQueue: utils.MessageQueue{},
		Clock: Clock{
			value: 0,
			mutex: sync.Mutex{},
		},
		Address: utils.ServerAddress{
			IP:   "localhost",
			Port: strconv.Itoa(BasePort + serverIndex),
		},
		Addresses: []utils.ServerAddress{},
		AddressToClient: utils.ServerAddress{
			IP:   "localhost",
			Port: strconv.Itoa(BasePortToClient + serverIndex),
		},
	}

	//Configuro gli indirizzi delle altre repliche del db
	for i := 0; i < NumReplicas; i++ {
		if i != serverIndex {
			newAddress := utils.ServerAddress{IP: "localhost", Port: strconv.Itoa(BasePort + i)}
			dbSequential.Addresses = append(dbSequential.Addresses, newAddress)
		}
	}

	// Ogni replica si mette in ascolto di update da parte delle altre repliche
	listener, err := net.Listen("tcp", dbSequential.Address.GetFullAddress())
	if err != nil {
		fmt.Println("Error while starting server: ", err)
		return
	}
	log.Printf("Replica listens on port %s", dbSequential.Address.Port)

	// Gestisce richieste di update da parte delle altre repliche su una goroutine
	go func(listener net.Listener) {
		for {
			// Accetta la connessione
			conn, err := listener.Accept()
			if err != nil {
				fmt.Println("Errore nell'accettare la connessione:", err)
				continue
			}
			go dbSequential.receive(conn)
		}
	}(listener)

	// Register a new rpc server and the struct we created above.
	// Only structs which implement datastore interface
	// are allowed to register themselves
	server := rpc.NewServer()
	err = server.RegisterName("Datastore", dbSequential)
	if err != nil {
		log.Fatal("Format of service datastore is not correct: ", err)
	}

	// Listen for incoming TCP packets on specified port
	rpcListener, err := net.Listen("tcp", dbSequential.AddressToClient.GetFullAddress())
	if err != nil {
		log.Fatal("Error while starting RPC server:", err)
	}
	log.Printf("RPC server listens on port %s", dbSequential.AddressToClient.Port)

	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			log.Fatal("Error while closing RPC server:", err)
		}
	}(rpcListener)

	// Allow RPC server to accept requests connections on the listener
	// and serve requests for each incoming connection.
	server.Accept(rpcListener)
}
