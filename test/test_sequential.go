package main

import (
	"dbService/utils"
)

// runComplexSequentialTest esegue test per verificare il rispetto della consistenza sequenziale
// nel caso in cui client diversi richiedano in concorrenza operazioni su un'unica chiave
func runSimpleSequentialTest() {
	clients := createClients()

	// Costruisce la lista di richieste che ogni client deve realizzare verso la rispettiva replica.
	// Le operazioni da svolgere non sono casuali, ma sono definite a priori così da poter verificare il perseguimento della consistenza voluta
	// CLIENT 0
	requests := []Request{
		{op: utils.PUT, key: "x", value: "a"},
		{op: utils.GET, key: "x"},
		{op: utils.GET, key: "x"},
	}

	clients[0].requests = requests

	// CLIENT 1
	requests = []Request{
		{op: utils.PUT, key: "x", value: "b"},
		{op: utils.DELETE, key: "x"},
		{op: utils.GET, key: "x"},
	}

	clients[1].requests = requests

	// CLIENT 2
	requests = []Request{
		{op: utils.PUT, key: "x", value: "c"},
		{op: utils.DELETE, key: "x"},
		{op: utils.GET, key: "x"},
	}

	clients[2].requests = requests

	// CLIENT 3
	requests = []Request{
		{op: utils.PUT, key: "x", value: "d"},
		{op: utils.GET, key: "x"},
		{op: utils.GET, key: "x"},
	}

	clients[3].requests = requests

	// Avvia i client
	launchClients(clients)

}

// runComplexSequentialTest esegue test per verificare il rispetto della consistenza sequenziale
// nel caso in cui client diversi richiedano in concorrenza operazioni su chiavi diverse
func runComplexSequentialTest() {
	clients := createClients()

	// Costruisce la lista di richieste che ogni client deve realizzare verso la rispettiva replica.
	// Le operazioni da svolgere non sono casuali, ma sono definite a priori così da poter verificare il perseguimento della consistenza voluta
	// CLIENT 0
	requests := []Request{
		{op: utils.PUT, key: "x", value: "a"},
		{op: utils.DELETE, key: "x"},
		{op: utils.GET, key: "z"},
		{op: utils.GET, key: "y"},
	}

	clients[0].requests = requests

	// CLIENT 1
	requests = []Request{
		{op: utils.PUT, key: "x", value: "b"},
		{op: utils.GET, key: "x"},
		{op: utils.PUT, key: "z", value: "e"},
		{op: utils.GET, key: "z"},
	}

	clients[1].requests = requests

	// CLIENT 2
	requests = []Request{
		{op: utils.PUT, key: "z", value: "c"},
		{op: utils.GET, key: "z"},
		{op: utils.PUT, key: "y", value: "d"},
		{op: utils.GET, key: "y"},
	}

	clients[2].requests = requests

	// CLIENT 3
	requests = []Request{
		{op: utils.PUT, key: "y", value: "a"},
		{op: utils.PUT, key: "z", value: "b"},
		{op: utils.DELETE, key: "y"},
		{op: utils.GET, key: "z"},
	}

	clients[3].requests = requests

	// Avvia i client
	launchClients(clients)
}
