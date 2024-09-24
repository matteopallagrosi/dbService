package main

import (
	"bufio"
	"dbService/utils"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

func main() {
	// Connessione al server RPC
	client, err := rpc.Dial("tcp", "localhost:8080")
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

			fmt.Print("Risultato: " + reply.Value)

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

			err := client.Call("Datastore.Put", args, &reply)
			if err != nil {
				log.Fatal("Error while executing PUT:", err)
			}

			fmt.Print("Risultato: " + reply.Value)

		default:
			fmt.Println("Scelta non valida, riprova.")
		}
	}
}
