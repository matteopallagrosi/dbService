package main

import (
	"dbService/utils"
	"encoding/json"
	"fmt"
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
	ID                 int                               // ID univoco del server
	DbStore            DbStore                           // Store di coppie chiave-valore
	MessageQueue       utils.VectorMessageQueue          // Coda di messaggi mantenuta dal server
	Clock              VectorClock                       // Clock vettoriale locale al server
	Address            utils.ServerAddress               // Indirizzo della replica (con cui può essere contattata dalle altre repliche)
	Addresses          []utils.ServerAddress             // Indirizzi delle altre repliche del db
	NextMessage        NextMessage                       // Tiene traccia dell'ID da assegnare al prossimo messaggio costruito
	AddressToClient    utils.ServerAddress               // Indirizzo con cui il server è contattato dai client
	FIFOQueues         map[int]*utils.VectorMessageQueue // Mantiene per ogni replica una coda per gestire la ricezione FIFO order dei messaggi
	ExpectedNextSeqNum map[int]*NextSeqNum               // Per ogni replica tiene traccia del numero di sequenza del messaggio successivo che deve ricevere da quella replica (comunicazione FIFO order)
	NextSeqNum         NextSeqNum                        // Numero di sequenza da assegnare al prossimo messaggio (REQUEST o ACK) inviato dal server
}

// Get recupera il valore corrispondente a una chiave
func (db *DbCausal) Get(args utils.Args, result *utils.Result) error {
	db.DbStore.getEntry(args.Key)
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

// updateVectorClockOnDelivery aggiorna il clock, per ogni k: V[k] = max{V[k], ts(msg)[k]}
func (db *DbCausal) updateVectorClockOnDelivery(msgClock []int) {
	for k := 0; k < len(db.Clock.value); k++ {
		if db.Clock.value[k] < msgClock[k] {
			db.Clock.value[k] = msgClock[k]
		}
	}
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

// sendVectorMessage invia un messaggio alle altre repliche, simulando un ritardo di comunicazione
func (db *DbCausal) sendVectorMessage(msg utils.VectorMessage) {
	// Assegna un numero di sequenza al messaggio da inviare
	// In questo modo il receiver può processare i messaggi da questo sender nello stesso ordine di invio
	// Il ritardo nella comunicazione è simulato inviando i messaggi allo scadere di un timer casuale
	seqNum := db.NextSeqNum.getNextSeqNum()
	msg.SeqNum = seqNum

	//Simula il ritardo di comunicazione
	simulateDelay()

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

// handleConnection gestisce la ricezione dei messaggi tenendo conto della garanzia di comunicazione FIFO order
func (db *DbCausal) handleConnection(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Fatal("Errore while closing connection :", err)
		}
	}(conn)

	// Decodifica il messaggio JSON ricevuto
	decoder := json.NewDecoder(conn)
	var msg utils.VectorMessage
	err := decoder.Decode(&msg)
	if err != nil {
		log.Fatal("Errore while decoding message:", err)
		return
	}

	seqNum := msg.SeqNum
	idSender := msg.ServerID

	// Controlla se il messaggio ricevuto dal server idSender ha il numero di sequenza atteso
	// se ha il numero di sequenza atteso il messaggio può essere ricevuto
	if db.ExpectedNextSeqNum[idSender].checkExpectedSeqNum(seqNum) != -1 {
		db.receive(msg)
		// Controlla se la ricezione in ordine del messaggio permette di processare i messaggi successivi in ordine FIFO
		checkNextMessage := true
		for checkNextMessage {
			db.ExpectedNextSeqNum[idSender].mutex.Lock()
			expectedSeqNum := db.ExpectedNextSeqNum[idSender].SeqNum
			resultMessage := db.FIFOQueues[idSender].PopNextSeqNumMessage(expectedSeqNum)
			if resultMessage != nil {
				db.ExpectedNextSeqNum[idSender].SeqNum++
				db.ExpectedNextSeqNum[idSender].mutex.Unlock()
				db.receive(*resultMessage)
			} else {
				db.ExpectedNextSeqNum[idSender].mutex.Unlock()
				checkNextMessage = false
			}
		}
	} else {
		//altrimenti il messaggio è inserito nella coda FIFO dei messaggi mandati dal sender idSender secondo il numero di sequenza
		db.FIFOQueues[idSender].InsertFIFOMessage(msg)
	}
}

// receive gestisce la ricezione di un messaggio da parte della replica
func (db *DbCausal) receive(msg utils.VectorMessage) {

	db.Clock.mutex.Lock()

	// controlla se sono rispettate le condizioni per fare la delivery del messaggio, ossia poter realizzare l'operazione associata
	if msg.CheckDelivery(db.Clock.value) {
		// se le condizioni sono rispettate
		// aggiorna il clockVettoriale sulla delivery del messaggio
		db.updateVectorClockOnDelivery(msg.Clock)
		db.Clock.mutex.Unlock()

		// processa il messaggio
		db.DeliverMessage(msg)

		// controlla se l'avvenuta delivery del messaggio, con conseguente update del clock vettoriale, permette di prelevare ulteriori messaggi nella coda di attesa
		db.Clock.mutex.Lock()
		resultMessage := db.MessageQueue.PopVectorMessage(db.Clock.value)
		for resultMessage != nil {
			db.updateVectorClockOnDelivery(resultMessage.Clock)
			db.DeliverMessage(*resultMessage)
			// controlla se ci sono ulteriori messaggi da estrarre dalla coda di attesa
			resultMessage = db.MessageQueue.PopVectorMessage(db.Clock.value)
		}
		db.Clock.mutex.Unlock()
	} else {
		db.Clock.mutex.Unlock()
		// se le condizioni non sono rispettate, inserisce il messaggio in una coda di attesa
		db.MessageQueue.AddMessage(msg)
	}
}

// DeliverMessage consegna il messaggio all'applicativo, ossia realizza l'operazione associata
func (db *DbCausal) DeliverMessage(msg utils.VectorMessage) {
	// processa il messaggio
	switch msg.Op {
	case utils.GET:
		db.DbStore.getEntry(msg.Key)
		fmt.Printf("\nGET: %s\n", msg.Key)
	case utils.PUT:
		db.DbStore.putEntry(msg.Key, msg.Value)
		fmt.Printf("\nPUT: %s %s\n", msg.Key, msg.Value)
		// Iterazione sulla mappa e stampa di ogni chiave e valore
		/*for key, value := range db.DbStore.Store {
			fmt.Printf("Chiave: %s, Valore: %s\n", key, value)
		}*/
	case utils.DELETE:
		db.DbStore.deleteEntry(msg.Key)
		fmt.Printf("\nDELETE: %s\n", msg.Key)
		// Iterazione sulla mappa e stampa di ogni chiave e valore
		/*for key, value := range db.DbStore.Store {
			if len(db.DbStore.Store) == 0 {
				println("store vuoto")
			} else {
				fmt.Printf("Chiave: %s, Valore: %s\n", key, value)
			}
		}*/
	}
}
