package main

import (
	"dbService/utils"
)

// runSimpleCausalTest esegue test per verificare il rispetto della consistenza causale.
// Il test simula il comportamento di 4 client che richiedono operazioni a diverse repliche del db
func runSimpleCausalTest() {
	// Crea i client, ognuno dei quali interagisce con una diversa replica del datastore
	clients := createClients()

	// Costruisce la lista di richieste che ogni client deve realizzare verso la rispettiva replica.
	// Le operazioni da svolgere non sono casuali, ma sono definite a priori così da poter verificare il perseguimento della consistenza voluta
	// CLIENT 0
	requests := []Request{
		{op: utils.PUT, key: "x", value: "a"},
		{op: utils.PUT, key: "x", value: "c"},
		{op: utils.GET, key: "x"},
	}

	clients[0].requests = requests

	// CLIENT 1
	requests = []Request{
		{op: utils.GET, key: "x"},
		{op: utils.PUT, key: "x", value: "b"},
		{op: utils.GET, key: "x"},
	}

	clients[1].requests = requests

	// CLIENT 2
	requests = []Request{
		{op: utils.PUT, key: "y", value: "b"},
		{op: utils.GET, key: "x"},
		{op: utils.GET, key: "x"},
	}

	clients[2].requests = requests

	// CLIENT 3
	requests = []Request{
		{op: utils.PUT, key: "z", value: "c"},
		{op: utils.GET, key: "x"},
		{op: utils.GET, key: "x"},
	}

	clients[3].requests = requests

	// Avvia i client
	launchClients(clients)
}

// runComplexCausalTest esegue test per verificare il rispetto della consistenza causale.
// Richiede l'esecuzione di molteplici scritture in relazione causa-effetto
func runComplexCausalTest() {
	clients := createClients()

	// CLIENT 0
	requests := []Request{
		{op: utils.GET, key: "x"},
		{op: utils.PUT, key: "y", value: "b"},
		{op: utils.PUT, key: "w", value: "a"},
		{op: utils.GET, key: "w"},
	}

	clients[0].requests = requests

	// CLIENT 1
	requests = []Request{
		{op: utils.PUT, key: "x", value: "a"},
		{op: utils.GET, key: "x"},
		{op: utils.PUT, key: "w", value: "c"},
		{op: utils.GET, key: "w"},
	}

	clients[1].requests = requests

	// CLIENT 2
	requests = []Request{
		{op: utils.GET, key: "y"},
		{op: utils.PUT, key: "y", value: "c"},
		{op: utils.GET, key: "z"},
		{op: utils.DELETE, key: "x"},
	}

	clients[2].requests = requests

	// CLIENT 3
	requests = []Request{
		{op: utils.PUT, key: "z", value: "a"},
		{op: utils.GET, key: "z"},
		{op: utils.PUT, key: "w", value: "b"},
		{op: utils.GET, key: "w"},
	}

	clients[3].requests = requests

	// Avvia i client
	launchClients(clients)
}
