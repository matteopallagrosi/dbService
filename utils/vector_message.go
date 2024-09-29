package utils

import "sync"

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
