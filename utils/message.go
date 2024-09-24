package utils

import (
	"fmt"
	"sort"
	"sync"
)

type Operation string

type MessageType string

// Operazioni che possono essere richieste al server
const (
	GET    Operation = "GET"
	PUT    Operation = "PUT"
	DELETE Operation = "DELETE"
)

// Operazioni che possono essere richieste al server
const (
	REQUEST MessageType = "REQUEST"
	ACK     MessageType = "ACK"
)

// Message rappresenta struttura del messaggio di REQUEST o di ACK
type Message struct {
	ID       int         `json:"id"` // ID univoco del messaggio, permette di associare gli ACK alle REQUEST
	Key      string      `json:"key"`
	Value    string      `json:"value"`
	Op       Operation   `json:"op"`
	Clock    int         `json:"clock"`
	Type     MessageType `json:"type"`
	ServerID int         `json:"server_id"` // ID del processo che propaga la REQUEST o l' ACK
}

// MessageQueue rappresenta la coda di messaggi mantenuta da ogni server
type MessageQueue struct {
	messages []Message
	mutex    sync.Mutex
}

// AddMessage aggiunge un messaggio alla coda e la mantiene ordinata in base al clock.
// A parità di clock ordina i messaggi in funzione di ProcessID
func (mq *MessageQueue) AddMessage(msg Message) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()
	mq.messages = append(mq.messages, msg)

	// Ordinare la coda prima per Clock e poi per ServerID (in caso di parità di Clock)
	sort.Slice(mq.messages, func(i, j int) bool {
		if mq.messages[i].Clock == mq.messages[j].Clock {
			return mq.messages[i].ServerID < mq.messages[j].ServerID
		}
		return mq.messages[i].Clock < mq.messages[j].Clock
	})
}

// PopMessage estrae il messaggio in testa se esiste almeno un messaggio con clock maggiore
// da ciascun altro server (ServerID diverso da quello del server che richiede la pop, ossia idRequester)
func (mq *MessageQueue) PopMessage(idRequester int, numReplicas int) *Message {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	if len(mq.messages) == 0 {
		return nil
	}

	// Estrae il messaggio in testa alla coda
	headMessage := mq.messages[0]

	// Mappa per tracciare se abbiamo trovato almeno un messaggio con clock maggiore per ogni ServerID diverso
	serverIDFound := make(map[int]bool)

	for i := 1; i < len(mq.messages); i++ {
		msg := mq.messages[i]

		// Consideriamo solo i messaggi con ServerID diverso da quello del server che richiede la pop
		if msg.ServerID != idRequester && msg.Clock > headMessage.Clock {
			serverIDFound[msg.ServerID] = true
		}
	}

	// Verifica se abbiamo trovato almeno un messaggio con clock maggiore per tutti i server diversi
	if len(serverIDFound) == (numReplicas - 1) {
		// Rimuovi il messaggio in testa
		mq.messages = mq.messages[1:]
		return &headMessage
	}

	// Se la condizione non è rispettata, ritorna nil
	return nil
}

// PrintQueue stampa lo stato della coda
func (mq *MessageQueue) PrintQueue() {
	for _, msg := range mq.messages {
		fmt.Printf("Message: %d, Clock: %d, ProcessID: %d\n", msg.ID, msg.Clock, msg.ServerID)
	}
}
