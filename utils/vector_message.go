package utils

import (
	"sort"
	"sync"
)

// VectorMessage rappresenta la struttura del messaggio scambiato nel multicast causalmente ordinato
type VectorMessage struct {
	Key      string    `json:"key"`
	Value    string    `json:"value"`
	Op       Operation `json:"op"`
	Clock    []int     `json:"clock"`
	ServerID int       `json:"server_id"` // ID del processo che propaga il messaggio
	SeqNum   int       `json:"seq_num"`   // Numero di sequenza che identifica l'ordine con cui partono i messaggi da un server
}

// VectorMessageQueue rappresenta la coda di messaggi mantenuta da ogni server
type VectorMessageQueue struct {
	messages []VectorMessage
	mutex    sync.Mutex
}

// AddMessage aggiunge un messaggio alla coda
func (mq *VectorMessageQueue) AddMessage(msg VectorMessage) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()
	mq.messages = append(mq.messages, msg)
}

// CheckDelivery controlla se il messaggio può essere consegnato all'applicativo (ossia la corrispondente operazione possa essere realizzata)
// oppure se debba essere inserito nella coda di messaggi per ritardarne la delivery.
// vectorClock in ingresso rappresenta il clockVettoriale del processo ricevente che deve decidere se realizzare oppure no la delivery del messaggio.
func (msg *VectorMessage) CheckDelivery(vectorClock []int) bool {
	msgClock := msg.Clock
	serverID := msg.ServerID // ID della replica i che ha inviato il messaggio

	// controlla che ts(msg)[i] = V(receiver)[i] + 1
	// ossia verifica che il messaggio ricevuto dalla replica i è il successivo messaggio che il ricevente si aspetta di ricevere
	if msgClock[serverID] != vectorClock[serverID]+1 {
		return false
	}

	// controlla che per ogni k diverso da i, ts(msg)[k] <= V(receiver)[k]
	// ossia controlla che il processo receiver abbia visto almeno tanti messaggi dalle diverse repliche quanti il sender i
	for k := 0; k < len(msgClock); k++ {
		if k != serverID && msgClock[k] > vectorClock[k] {
			return false
		}
	}

	// Se tutte le condizioni sono rispettate, il messaggio può essere consegnato
	return true
}

// PopVectorMessage estrae un messaggio dalla coda di attesa (il primo che incontra) se sono rispettate le condizioni del multicast causalmente ordinato
func (mq *VectorMessageQueue) PopVectorMessage(vectorClock []int) *VectorMessage {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	for i, msg := range mq.messages {
		// se le condizioni sono rispettate
		if msg.CheckDelivery(vectorClock) {
			// rimuove il messaggio dalla coda e lo restituisce
			deliverableMessage := msg
			mq.messages = append(mq.messages[:i], mq.messages[i+1:]...)
			return &deliverableMessage
		}
	}

	return nil
}

// InsertFIFOMessage inserisce il messaggio in una coda ordinata per numero si sequenza
// Questo permette di garantire comunicazione FIFO order per ogni coppia di server i-j
func (mq *VectorMessageQueue) InsertFIFOMessage(msg VectorMessage) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()
	mq.messages = append(mq.messages, msg)

	// Ordina la coda per numero di sequenza
	sort.Slice(mq.messages, func(i, j int) bool {
		return mq.messages[i].SeqNum < mq.messages[j].SeqNum
	})
}

// PopNextSeqNumMessage estrae il messaggio in testa alla coda se il numero di sequenza coincide con quello atteso
func (mq *VectorMessageQueue) PopNextSeqNumMessage(nextSeqNum int) *VectorMessage {
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
