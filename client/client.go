package main

import (
	"bufio"
	"dbService/utils"
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	NumReplicas int
	BasePort    int
)

func init() {
	// Carica le variabili d'ambiente dal file .env
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Recupera e converte le variabili d'ambiente
	NumReplicas, _ = strconv.Atoi(os.Getenv("NUM_REPLICAS"))
	BasePort, _ = strconv.Atoi(os.Getenv("BASE_PORT_TO_CLIENT"))
}

func main() {

	serverIndex := GetRandomIndex()
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

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("Scegli l'operazione:")
		fmt.Println("1. GET")
		fmt.Println("2. PUT")
		fmt.Println("3. DELETE")
		fmt.Print("Inserisci un numero (1, 2, 3): ")

		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		choice, err := strconv.Atoi(input)
		if err != nil {
			fmt.Println("Errore: devi inserire un numero valido!")
			continue
		}

		args := utils.Args{}
		var reply utils.Result

		switch choice {
		case 1: // GET
			fmt.Print("Inserisci la chiave: ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)

			args.Key = key

			err := client.Call("Datastore.Get", args, &reply)
			if err != nil {
				log.Fatal("Error while executing GET:", err)
			}

			fmt.Print("Risultato: " + reply.Value + "\n")

		case 2: // PUT
			fmt.Print("Inserisci la chiave: ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)

			fmt.Print("Inserisci il valore: ")
			value, _ := reader.ReadString('\n')
			value = strings.TrimSpace(value)

			args.Key = key
			args.Value = value

			err := client.Call("Datastore.Put", args, &reply)
			if err != nil {
				log.Fatal("Error while executing PUT:", err)
			}

			fmt.Print("Risultato: " + reply.Value)

		case 3: // DELETE
			fmt.Print("Inserisci la chiave: ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)

			args.Key = key

			err := client.Call("Datastore.Delete", args, &reply)
			if err != nil {
				log.Fatal("Error while executing DELETE:", err)
			}

			fmt.Print("Risultato: " + reply.Value)

		default:
			fmt.Println("Scelta non valida, riprova.")
		}
	}
}

// GetRandomIndex ritorna un indice casuale tra 0 e NumReplicas - 1 (inclusi)
func GetRandomIndex() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(NumReplicas)

}
