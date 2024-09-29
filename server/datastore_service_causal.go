package main

import (
	"dbService/utils"
	"encoding/json"
	"log"
	"net"
	"sync"
)

type VectorClock struct {
	value []int
	mutex sync.Mutex
}

// DbCausal fornisce il servizio di gestione del db key-value.
// Garantisce consistenza causale, tramite multicast causalmente ordinato
type DbCausal struct {
	ID                 int                         // ID univoco del server
	DbStore            DbStore                     // Store di coppie chiave-valore
	MessageQueue       utils.MessageQueue          // Coda di messaggi mantenuta dal server
	Clock              VectorClock                 // Clock vettoriale locale al server
	Address            utils.ServerAddress         // Indirizzo della replica (con cui può essere contattata dalle altre repliche)
	Addresses          []utils.ServerAddress       // Indirizzi delle altre repliche del db
	NextMessage        NextMessage                 // Tiene traccia dell'ID da assegnare al prossimo messaggio costruito
	AddressToClient    utils.ServerAddress         // Indirizzo con cui il server è contattato dai client
	FIFOQueues         map[int]*utils.MessageQueue // Mantiene per ogni replica una coda per gestire la ricezione FIFO order dei messaggi
	ExpectedNextSeqNum map[int]*NextSeqNum         // Per ogni replica tiene traccia del numero di sequenza del messaggio successivo che deve ricevere da quella replica (comunicazione FIFO order)
	NextSeqNum         NextSeqNum                  // Numero di sequenza da assegnare al prossimo messaggio (REQUEST o ACK) inviato dal server
}

// Get recupera il valore corrispondente a una chiave
func (db *DbCausal) Get(args utils.Args, result *utils.Result) error {
	//TODO
	return nil
}

// Put inserisce una nuova coppia key-value, o aggiorna il valore corrente se la chiave già esiste
func (db *DbCausal) Put(args utils.Args, result *utils.Result) error {
	// La consegna all'applicativo di un messaggio proveniente dal processo stesso può essere realizzata immediatamente.
	// Questo perché eventi successivi in uno stesso processo sono causalmente ordinati tra loro, nell'ordine con cui tali richieste giungono alla replica.
	db.DbStore.putEntry(args.Key, args.Value)

	// propaga la PUT verso le altre repliche del db
	db.sendUpdate(utils.PUT, args.Key, args.Value)
	return nil
}

// Delete rimuove la entry corrispondente a una data chiave
func (db *DbCausal) Delete(args utils.Args, result *utils.Result) error {
	//Sono valide le stesse considerazioni realizzate per la PUT.
	db.DbStore.deleteEntry(args.Key)

	//propaga la DELETE verso le altre repliche del db
	db.sendUpdate(utils.DELETE, args.Key, args.Value)
	return nil
}

// updateVectorClockOnSend incrementa di 1 il clock del server nel clock vettoriale
func (db *DbCausal) updateVectorClockOnSend() {
	db.Clock.mutex.Lock()
	db.Clock.value[db.ID]++
	db.Clock.mutex.Unlock()
}

// sendUpdate propaga la richiesta di update (PUT o DELETE) verso gli altri processi
func (db *DbCausal) sendUpdate(op utils.Operation, key string, value string) {

	// Incrementa il clock del server di 1
	db.updateVectorClockOnSend()

	// costruisce un messaggio associato alla richiesta di update, associando il clock vettoriale
	update := utils.VectorMessage{
		Key:      key,
		Value:    value,
		Op:       op,
		Clock:    db.Clock.value,
		ServerID: db.ID,
	}

	// Invia il messaggio alle altre repliche, simulando un ritardo di comunicazione
	db.sendVectorMessage(update)
}

// Invia un messaggio alle altre repliche, simulando un ritardo di comunicazione
func (db *DbCausal) sendVectorMessage(msg utils.VectorMessage) {
	// Assegna un numero di sequenza al messaggio da inviare
	// In questo modo il receiver può processare i messaggi da questo sender nello stesso ordine di invio
	// Il ritardo nella comunicazione è simulato inviando i messaggi allo scadere di un timer casuale
	seqNum := db.NextSeqNum.getNextSeqNum()
	msg.SeqNum = seqNum

	//Simula il ritardo di comunicazione
	//simulateDelay()

	for _, address := range db.Addresses {
		conn, err := net.Dial("tcp", address.GetFullAddress())
		if err != nil {
			log.Fatal("Error in dialing: ", err)
		}

		// Codifica il messaggio in json e lo invia al server
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

func (db *DbCausal) receive(msg utils.VectorMessage) {
	//TODO
}
