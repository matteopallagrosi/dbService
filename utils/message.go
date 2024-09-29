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

type MessageIdentifier struct {
	ID       int `json:"id"`       // ID del messaggio univoco nella replica del db
	ServerId int `json:"serverId"` // Identificatore del server, in cui l'ID del messaggio è univoco

}

// Message rappresenta struttura del messaggio di REQUEST o di ACK
type Message struct {
	MessageID MessageIdentifier `json:"identifier"` // Identificatore univoco del messaggio, permette di associare gli ACK alle REQUEST
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Op        Operation         `json:"op"`
	Clock     int               `json:"clock"`
	Type      MessageType       `json:"type"`
	ServerID  int               `json:"server_id"` // ID del processo che propaga la REQUEST o l' ACK
	SeqNum    int               `json:"seq_num"`   // Numero di sequenza che identifica l'ordine con cui partono i messaggi da un server
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
	// Se il messaggio è un ACK, per costruzione deve essere un ACK di un messaggio già processato arrivato in ritardo, quindi può essere scartato.
	// La logica dell'algoritmo prevede che un ACK deve sempre avere un clock maggiore del messaggio di cui realizza l' acknowledgment.
	// Quindi se l' ACK si trova in testa alla coda, il messaggio di REQUEST corrispondente doveva avere un clock inferiore, ed è quindi stato già estratto dalla coda.
	for headMessage.Type == ACK {
		mq.messages = mq.messages[1:]
		if len(mq.messages) == 0 {
			return nil
		}
		headMessage = mq.messages[0]
	}

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

// PopGetMessage estrae il messaggio in testa solo se è di tipo GET
func (mq *MessageQueue) PopGetMessage() *Message {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	// Verifica se ci sono messaggi nella coda
	if len(mq.messages) == 0 {
		return nil
	}

	// Prende il messaggio in testa
	headMessage := mq.messages[0]

	// Verifica se il messaggio in testa è di tipo GET
	if headMessage.Op == GET {
		// Rimuove il messaggio in testa dalla coda
		mq.messages = mq.messages[1:]

		// Ritorna il messaggio estratto
		return &headMessage
	}

	// Se il messaggio in testa non è di tipo GET, ritorna nil
	return nil
}

// DeleteAck rimuovere tutti gli ACK associati al messaggio con un dato ID, propagato a partire da un certo server.
// Il confronto con ServerId è necessario in quanto i messageId sono univoci solo all'interno di ciascuna replica, non globalmente
func (mq *MessageQueue) DeleteAck(messageId int, serverId int) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	// Filtra i messaggi, rimuovendo quelli di tipo ACK con l' ID dato in ingresso
	filteredMessages := mq.messages[:0]

	for _, msg := range mq.messages {
		if !(msg.Type == ACK && msg.MessageID.ID == messageId && msg.MessageID.ServerId == serverId) {
			filteredMessages = append(filteredMessages, msg)
		}
	}

	// Aggiorna la coda con i messaggi filtrati
	mq.messages = filteredMessages
}

// InsertFIFOMessage inserisce il messaggio in una coda ordinata per numero si sequenza
// Questo permette di garantire comunicazione FIFO order per ogni coppia di server i-j
func (mq *MessageQueue) InsertFIFOMessage(msg Message) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()
	mq.messages = append(mq.messages, msg)

	// Ordinare la coda prima per numero di sequenza
	sort.Slice(mq.messages, func(i, j int) bool {
		return mq.messages[i].SeqNum < mq.messages[j].SeqNum
	})
}

// PrintQueue stampa lo stato della coda
func (mq *MessageQueue) PrintQueue() {
	if len(mq.messages) == 0 {
		println("coda vuota")
	}
	for _, msg := range mq.messages {
		fmt.Printf("Message: %d, From: %d, Type: %s, Clock: %d, ProcessID: %d\n", msg.MessageID.ID, msg.MessageID.ServerId, msg.Type, msg.Clock, msg.ServerID)
	}
}

// PopNextSeqNumMessage estrae il messaggio in testa alla coda se il numero di sequenza coincide con quello atteso
func (mq *MessageQueue) PopNextSeqNumMessage(nextSeqNum int) *Message {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	// Verifica che ci sia almeno un messaggio in coda
	if len(mq.messages) == 0 {
		return nil // Non ci sono messaggi in coda
	}

	// Controlla se il SeqNum del messaggio in testa è uguale a nextSeqNum
	if mq.messages[0].SeqNum == nextSeqNum {
		// Estrae il messaggio in testa
		message := mq.messages[0]

		// Rimuove il messaggio dalla coda
		mq.messages = mq.messages[1:]

		// Ritorna il messaggio
		return &message
	}

	// Se il SeqNum non corrisponde, non rimuovere nulla e ritorna nil
	return nil
}
