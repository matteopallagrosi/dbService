package main

import (
	"dbService/utils"
	"github.com/joho/godotenv"
	"log"
	"net/rpc"
	"os"
	"strconv"
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
	BasePort         int
	BasePortToClient int
	ConsistencyType  string
)

func init() {
	// Carica le variabili d'ambiente dal file .env
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Recupera e converte le variabili d'ambiente
	NumReplicas, _ = strconv.Atoi(os.Getenv("NUM_REPLICAS"))
	BasePort, _ = strconv.Atoi(os.Getenv("BASE_PORT"))
	BasePortToClient, _ = strconv.Atoi(os.Getenv("BASE_PORT_TO_CLIENT"))
	ConsistencyType = os.Getenv("CONSISTENCY_TYPE")
}

func main() {
	// Simula l'esecuzione concorrente di molteplici client.
	// Viene controllata la corretta realizzazione della consistenza sequenziale o causale, in base al valore della variabile d'ambiente
	if ConsistencyType == "SEQUENTIAL" {
		// simula interazione con repliche del db che garantiscono consistenza sequenziale
		runSequentialTest()
	} else if ConsistencyType == "CAUSAL" {
		// simula interazione con repliche del db che garantiscono consistenza causale
		runCausalTest()
	}
}

// LaunchRequest esegue la lista di richieste definita per ogni client
func (client *Client) LaunchRequest() {
	for _, request := range client.requests {
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
	}
}
