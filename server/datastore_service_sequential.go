package main

import (
	"dbService/utils"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
)

type Clock struct {
	value int
	mutex sync.Mutex
}

type NextMessage struct {
	ID    int
	mutex sync.Mutex
}

// DbSequential fornisce il servizio di gestione del db key-value.
// Garantisce consistenza sequenziale, tramite multicast totalmente ordinato
type DbSequential struct {
	ID              int                   // ID univoco del server
	DbStore         DbStore               // Store di coppie chiave-valore
	MessageQueue    utils.MessageQueue    // Coda di messaggi mantenuta dal server
	Clock           Clock                 // Clock scalare locale al server
	Address         utils.ServerAddress   // Indirizzo della replica (con cui può essere contattata dalle altre repliche)
	Addresses       []utils.ServerAddress // Indirizzi delle altre repliche del db
	NextMessage     NextMessage           // Tiene traccia dell'ID da assegnare al prossimo messaggio costruito
	AddressToClient utils.ServerAddress   // Indirizzo con cui il server è contattato dai client
}

type DbStore struct {
	Store map[string]string // Store di coppie chiave-valore
	mutex sync.Mutex
}

// Get recupera il valore corrispondente a una chiave
func (db *DbSequential) Get(args utils.Args, result *utils.Result) error {
	//TODO
	return nil
}

// Put inserisce una nuova coppia key-value, o aggiorna il valore corrente se la chiave già esiste
func (db *DbSequential) Put(args utils.Args, result *utils.Result) error {
	// propaga la PUT verso le altre repliche del db
	db.sendUpdate(utils.PUT, args.Key, args.Value)
	return nil
}

// Delete rimuove la entry corrispondente a una data chiave
func (db *DbSequential) Delete(args utils.Args, result *utils.Result) error {
	//propaga la DELETE verso le altre repliche del db
	db.sendUpdate(utils.DELETE, args.Key, args.Value)
	return nil
}

// updateClockOnSend incrementa di 1 il clock scalare
func (db *DbSequential) updateClockOnSend() {
	db.Clock.mutex.Lock()
	db.Clock.value++
	db.Clock.mutex.Unlock()
}

// updateClockOnReceive configura il clock corrente al max(msg.clock, currentValue).
// Poi incrementa il clock di 1
func (db *DbSequential) updateClockOnReceive(msgClock int) {
	db.Clock.mutex.Lock()
	if msgClock > db.Clock.value {
		db.Clock.value = msgClock
	}
	db.Clock.value++
	db.Clock.mutex.Unlock()
}

// sendUpdate propaga la richiesta di update (PUT o DELETE) verso gli altri processi
func (db *DbSequential) sendUpdate(op utils.Operation, key string, value string) {

	// Incrementa il clock di 1
	db.updateClockOnSend()

	// Recupera l' ID del prossimo messaggio
	nextID := db.getNextMessageID()

	// costruisce un messaggio associato alla richiesta di update
	update := utils.Message{
		MessageID: utils.MessageIdentifier{
			ID:       nextID,
			ServerId: db.ID,
		},
		Key:      key,
		Value:    value,
		Op:       op,
		Clock:    db.Clock.value,
		Type:     utils.REQUEST,
		ServerID: db.ID,
	}

	// Aggiunge il messaggio alla coda di messaggi, ordinata per clock (e serverID a parità di clock)
	// A livello concettuale il sender invia il messaggio a se stesso
	db.MessageQueue.AddMessage(update)

	// Invia il messaggio alle altre repliche, simulando un ritardo di comunicazione
	// A livello concettuale è come se il sender inviasse il messaggio anche a se stesso
	// Nella pratica il sender non realizza l'invio del messaggio perché già lo possiede
	db.sendMessage(update)

	// Poiché a livello concettuale il sender invia il messaggio anche a se stesso, anche lui invia l' ACK a tutte le altre repliche
	db.sendAck(update)

}

func (db *DbSequential) sendAck(msg utils.Message) {
	// Incrementa il clock di 1
	db.updateClockOnSend()

	// Costruisce il messaggio di ACK da inviare alle altre repliche
	// questo messaggio presenta come identificatore lo stesso identificatore del messaggio di cui realizza l' acknowledgment
	ack := utils.Message{
		MessageID: utils.MessageIdentifier{
			ID:       msg.MessageID.ID,
			ServerId: msg.MessageID.ServerId,
		},
		Key:      "",
		Value:    "",
		Op:       "",
		Clock:    db.Clock.value,
		Type:     utils.ACK,
		ServerID: db.ID,
	}

	// Invia l' ACK alle altre repliche
	db.sendMessage(ack)
}

// Invia un messaggio (REQUEST o ACK) alle altre repliche, simulando un ritardo di comunicazione
func (db *DbSequential) sendMessage(msg utils.Message) {
	for _, address := range db.Addresses {
		conn, err := net.Dial("tcp", address.GetFullAddress())
		if err != nil {
			log.Fatal("Error in dialing: ", err)
		}

		// Codifica il messaggio in json e lo invia al server
		// NETWORKDELAY
		encoder := json.NewEncoder(conn)
		err = encoder.Encode(msg)
		if err != nil {
			log.Fatal("Error while coding message : ", err)
			return
		}

		err = conn.Close()
		if err != nil {
			log.Fatal("Error while closing connection : ", err)
		}
	}
}

// getNextMessageID recupera l'ID del prossimo messaggio costruito e propagato dal server
func (db *DbSequential) getNextMessageID() int {
	db.NextMessage.mutex.Lock()
	nextID := db.NextMessage.ID
	db.NextMessage.ID++
	db.NextMessage.mutex.Unlock()
	return nextID
}

// receive gestisce la ricezione di messaggi dalle altre repliche (che possono essere REQUEST o ACK)
func (db *DbSequential) receive(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Fatal("Errore while closing connection :", err)
		}
	}(conn)

	// Decodifica il messaggio JSON ricevuto
	decoder := json.NewDecoder(conn)
	var msg utils.Message
	err := decoder.Decode(&msg)
	if err != nil {
		log.Fatal("Errore while decoding message:", err)
		return
	}

	// Stampa il messaggio ricevuto
	fmt.Printf("\nRicevuto:\nID = %d\nDa = %d\nKey = %s\nValue = %s\nOperation = %s\nClock = %d\nType = %s\nServerID = %d\n",
		msg.MessageID.ID, msg.MessageID.ServerId, msg.Key, msg.Value, msg.Op, msg.Clock, msg.Type, msg.ServerID)

	// aggiorna il clock sulla ricezione
	db.updateClockOnReceive(msg.Clock)

	// processa il messaggio ricevuto
	switch msg.Type {
	case utils.REQUEST:
		db.MessageQueue.AddMessage(msg)
		//manda un ack di avvenuta ricezione del messaggio a tutte le altre repliche
		db.sendAck(msg)
	case utils.ACK:
		db.MessageQueue.AddMessage(msg)
	}

	// controlla se l'arrivo di questo messaggio permette di processare il messaggio in testa alla coda
	resultMessage := db.MessageQueue.PopMessage(db.ID, NumReplicas)
	if resultMessage != nil {
		switch resultMessage.Op {
		case utils.PUT:
			db.putEntry(resultMessage.Key, resultMessage.Value)
			fmt.Printf("\nPUT: %s %s\n", resultMessage.Key, resultMessage.Value)
			// Iterazione sulla mappa e stampa di ogni chiave e valore
			for key, value := range db.DbStore.Store {
				fmt.Printf("Chiave: %s, Valore: %s\n", key, value)
			}
		case utils.DELETE:
			db.deleteEntry(resultMessage.Key)
			fmt.Printf("\nDELETE: %s\n", resultMessage.Key)
			// Iterazione sulla mappa e stampa di ogni chiave e valore
			for key, value := range db.DbStore.Store {
				if len(db.DbStore.Store) == 0 {
					println("store vuoto")
				} else {
					fmt.Printf("Chiave: %s, Valore: %s\n", key, value)
				}
			}
		}

		db.MessageQueue.PrintQueue()

		// Dopo aver realizzato l'operazione contenuta nel messaggio provvede a eliminare tutti gli ACK associati dalla coda
		db.MessageQueue.DeleteAck(resultMessage.MessageID.ID, resultMessage.MessageID.ServerId)

		db.MessageQueue.PrintQueue()
	}
}

// putEntry inserisce una nuova entry nello store key-value.
// Se esiste già una entry nello store associata alla chiave data, il valore corrispondente viene aggiornato.
func (db *DbSequential) putEntry(key string, value string) {
	db.DbStore.mutex.Lock()
	defer db.DbStore.mutex.Unlock()
	db.DbStore.Store[key] = value
}

// deleteEntry rimuove la entry associata a una data chiave nello store key-value.
// Se la chiave non esiste la delete non esegue alcuna operazione
func (db *DbSequential) deleteEntry(key string) {
	db.DbStore.mutex.Lock()
	defer db.DbStore.mutex.Unlock()
	delete(db.DbStore.Store, key)
}
