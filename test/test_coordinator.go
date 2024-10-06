package main

import (
	"dbService/utils"
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Client struct {
	ID        int         // ID univoco del client
	rpcClient *rpc.Client // Client RPC che permette di interagire con il datastore
	requests  []Request   // Lista di richieste che il client inoltra alla replica a cui è connesso
}

type Request struct {
	op    utils.Operation
	key   string
	value string
}

var (
	NumReplicas      int
	BasePortToClient int
	ConsistencyType  string
	Test             string
	BaseName         string
	Container        bool
)

func init() {
	// Carica le variabili d'ambiente dal file .env
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Recupera e converte le variabili d'ambiente
	NumReplicas, _ = strconv.Atoi(os.Getenv("NUM_REPLICAS"))
	BasePortToClient, _ = strconv.Atoi(os.Getenv("BASE_PORT_TO_CLIENT"))
	ConsistencyType = os.Getenv("CONSISTENCY_TYPE")
	Test = os.Getenv("TEST")
	BaseName = os.Getenv("BASE_NAME")
	if os.Getenv("CONTAINER") == "YES" {
		Container = true
	} else {
		Container = false
	}
}

// Simula l'esecuzione concorrente di molteplici client.
// Viene controllata la corretta realizzazione della consistenza sequenziale o causale, in base al valore della variabile d'ambiente
func main() {
	if ConsistencyType == "SEQUENTIAL" {
		// Esegue i test per la consistenza sequenziale
		if Test == "SIMPLE" {
			fmt.Println("Running simple sequential test...")
			runSimpleSequentialTest()
		} else if Test == "COMPLEX" {
			fmt.Println("Running complex sequential test...")
			runComplexSequentialTest()
		} else {
			log.Fatal("Wrong test required, please use SIMPLE or COMPLEX")
		}
	} else if ConsistencyType == "CAUSAL" {
		// Esegue i test per la consistenza causale
		if Test == "SIMPLE" {
			fmt.Println("Running simple causal test...")
			runSimpleCausalTest()
		} else if Test == "COMPLEX" {
			fmt.Println("Running complex causal test...")
			runComplexCausalTest()
		} else {
			log.Fatal("Wrong test required, please use SIMPLE or COMPLEX")
		}
	} else {
		log.Fatal("Wrong consistency required, please use SEQUENTIAL or CAUSAL")
	}
}

// LaunchRequest esegue la lista di richieste definita per ogni client
func (client *Client) LaunchRequest(wg *sync.WaitGroup) {
	defer wg.Done()
	for _, request := range client.requests {
		// delay random prima di richiedere l'operazione successiva del client corrente
		randomDelay()
		args := utils.Args{
			Key:   request.key,
			Value: request.value,
		}

		var reply utils.Result

		var serviceMethod string = string("Datastore." + request.op)
		// Richiede l'operazione al db
		err := client.rpcClient.Call(serviceMethod, args, &reply)
		if err != nil {
			log.Fatal("Error while executing op: ", err)
		}
		if request.op == utils.GET {
			fmt.Printf("[CLIENT %d] GET key %s value %s\n", client.ID, request.key, reply.Value)
		}
	}

	// Attende 10 secondi così da garantire che tutte le repliche abbiano terminato di propagare i messaggi
	time.Sleep(time.Duration(10000) * time.Millisecond)

}

func createClients() []*Client {
	// Crea i client, ognuno dei quali interagisce con una diversa replica del datastore
	var clients []*Client
	for i := 0; i < NumReplicas; i++ {
		var serverAddress utils.ServerAddress
		if Container {
			serverAddress = utils.ServerAddress{IP: BaseName + "-" + strconv.Itoa(i), Port: strconv.Itoa(BasePortToClient)}
		} else {
			serverAddress = utils.ServerAddress{IP: "localhost", Port: strconv.Itoa(BasePortToClient + i)}
		}
		// Il client si collega al server RPC
		rpcClient, err := rpc.Dial("tcp", serverAddress.GetFullAddress())
		if err != nil {
			log.Fatal("Error in dialing: ", err)
		}

		fmt.Printf("Client %d connesso al server %s\n", i, serverAddress.GetFullAddress())
		client := &Client{
			ID:        i,
			rpcClient: rpcClient,
			requests:  nil,
		}

		//aggiunge il client alla lista dei client
		clients = append(clients, client)
	}
	return clients
}

func launchClients(clients []*Client) {
	// I client vengono lanciati in parallelo, ciascuno su una diversa goroutine, e vengono eseguite le rispettive richieste verso il db
	var wg sync.WaitGroup
	for i := 0; i < NumReplicas; i++ {
		wg.Add(1)
		go clients[i].LaunchRequest(&wg)
	}

	// Attende che i diversi client abbiano inviato le operazioni richieste
	wg.Wait()

	// Chiude le connessioni dei client
	for i := 0; i < NumReplicas; i++ {
		err := clients[i].rpcClient.Close()
		if err != nil {
			log.Fatal("Error while closing connection:", err)
		}
	}

	fmt.Println("All clients completed")
}

func randomDelay() {
	time.Sleep(time.Duration(2000+rand.Intn(4000)) * time.Millisecond) // ritardo tra 500ms e 2s
}
