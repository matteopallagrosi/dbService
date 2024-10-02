package main

import (
	"dbService/utils"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

type Clock struct {
	value int
	mutex sync.Mutex
}

type NextMessage struct {
	ID    int
	mutex sync.Mutex
}

type NextSeqNum struct {
	SeqNum int
	mutex  sync.Mutex
}

// DbSequential fornisce il servizio di gestione del db key-value.
// Garantisce consistenza sequenziale, tramite multicast totalmente ordinato
type DbSequential struct {
	ID                 int                         // ID univoco del server
	DbStore            DbStore                     // Store di coppie chiave-valore
	MessageQueue       utils.MessageQueue          // Coda di messaggi mantenuta dal server
	Clock              Clock                       // Clock scalare locale al server
	Address            utils.ServerAddress         // Indirizzo della replica (con cui può essere contattata dalle altre repliche)
	Addresses          []utils.ServerAddress       // Indirizzi delle altre repliche del db
	NextMessage        NextMessage                 // Tiene traccia dell'ID da assegnare al prossimo messaggio costruito
	AddressToClient    utils.ServerAddress         // Indirizzo con cui il server è contattato dai client
	FIFOQueues         map[int]*utils.MessageQueue // Mantiene per ogni replica una coda per gestire la ricezione FIFO order dei messaggi
	ExpectedNextSeqNum map[int]*NextSeqNum         // Per ogni replica tiene traccia del numero di sequenza del messaggio successivo che deve ricevere da quella replica (comunicazione FIFO order)
	NextSeqNum         NextSeqNum                  // Numero di sequenza da assegnare al prossimo messaggio (REQUEST o ACK) inviato dal server

}

// Get recupera il valore corrispondente a una chiave
func (db *DbSequential) Get(args utils.Args, result *utils.Result) error {
	db.handleGetRequest(args.Key)
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

// handleGetRequest gestisce la richiesta di GET da parte di un client.
// Nel caso della GET, a differenza di PUT e DELETE, il server non deve propagare la richiesta alle altre repliche.
// GET è considerato un evento interno al server.
// Poiché la GET è un evento interno, e quindi non è un messaggio proveniente da un'altra replica, non può innescare la possibilità di processare un qualche messaggio nella coda.
func (db *DbSequential) handleGetRequest(key string) {

	// Incrementa il clock di 1 anche nel caso di evento interno
	db.updateClockOnSend()

	// Recupera l' ID del prossimo messaggio
	nextID := db.getNextMessageID()

	// costruisce un messaggio associato alla richiesta di GET
	getMessage := utils.Message{
		MessageID: utils.MessageIdentifier{
			ID:       nextID,
			ServerId: db.ID,
		},
		Key:      key,
		Op:       utils.GET,
		Clock:    db.Clock.value,
		Type:     utils.REQUEST,
		ServerID: db.ID,
	}

	// Aggiunge il messaggio alla coda di messaggi, ordinata per clock (e serverID a parità di clock)
	db.MessageQueue.AddMessage(getMessage)

	//Se il messaggio di GET è in testa alla coda può essere immediatamente processato
	resultMessage := db.MessageQueue.PopGetMessage()
	if resultMessage != nil {
		// esegue l'operazione di GET richiesta
		db.DbStore.getEntry(resultMessage.Key)
		fmt.Printf("\nGET: %s\n", resultMessage.Key)
	}
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

// getNextMessageID recupera l'ID del prossimo messaggio costruito e propagato dal server
func (db *DbSequential) getNextMessageID() int {
	db.NextMessage.mutex.Lock()
	nextID := db.NextMessage.ID
	db.NextMessage.ID++
	db.NextMessage.mutex.Unlock()
	return nextID
}

// getNextSeqNum produce il numero di sequenza del messaggio successivo propagato dal server
func (seqNum *NextSeqNum) getNextSeqNum() int {
	seqNum.mutex.Lock()
	nextSeqNum := seqNum.SeqNum
	seqNum.SeqNum++
	seqNum.mutex.Unlock()
	return nextSeqNum
}

// checkExpectedSeqNum controlla se il numero di sequenza del messaggio ricevuto è quello atteso.
// In tal caso incrementa il numero di sequenza atteso per il messaggio successivo.
func (seqNum *NextSeqNum) checkExpectedSeqNum(msgSeqNum int) int {
	seqNum.mutex.Lock()
	expectedSeqNum := seqNum.SeqNum
	if expectedSeqNum == msgSeqNum {
		seqNum.SeqNum++
		seqNum.mutex.Unlock()
		return expectedSeqNum
	}
	seqNum.mutex.Unlock()
	return -1
}

// handleConnection gestisce la ricezione dei messaggi tenendo conto della garanzia di comunicazione FIFO order
func (db *DbSequential) handleConnection(conn net.Conn) {
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

// receive gestisce la ricezione di messaggi dalle altre repliche (che possono essere REQUEST o ACK)
func (db *DbSequential) receive(msg utils.Message) {
	// Stampa il messaggio ricevuto
	//fmt.Printf("\nRicevuto:\nID = %d\nDa = %d\nKey = %s\nValue = %s\nOperation = %s\nClock = %d\nType = %s\nServerID = %d\n",
	//	msg.MessageID.ID, msg.MessageID.ServerId, msg.Key, msg.Value, msg.Op, msg.Clock, msg.Type, msg.ServerID)

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
		// Dopo aver estratto il messaggio provvede a eliminare tutti gli ACK associati dalla coda (non presenti se il messaggio è una GET)
		if resultMessage.Op != utils.GET {
			db.MessageQueue.DeleteAck(resultMessage.MessageID.ID, resultMessage.MessageID.ServerId)
		}
		fmt.Printf("sto facendo op\n")
		switch resultMessage.Op {
		case utils.GET:
			db.DbStore.getEntry(resultMessage.Key)
			fmt.Printf("\nGET: %s\n", resultMessage.Key)
		case utils.PUT:
			db.DbStore.putEntry(resultMessage.Key, resultMessage.Value)
			fmt.Printf("\nPUT: %s %s\n", resultMessage.Key, resultMessage.Value)
			// Iterazione sulla mappa e stampa di ogni chiave e valore
			/*for key, value := range db.DbStore.Store {
				fmt.Printf("Chiave: %s, Valore: %s\n", key, value)
			}*/
		case utils.DELETE:
			db.DbStore.deleteEntry(resultMessage.Key)
			fmt.Printf("\nDELETE: %s\n", resultMessage.Key)
			// Iterazione sulla mappa e stampa di ogni chiave e valore
			/*for key, value := range db.DbStore.Store {
				if len(db.DbStore.Store) == 0 {
					println("store vuoto")
				} else {
					fmt.Printf("Chiave: %s, Valore: %s\n", key, value)
				}
			}*/
		}
		//db.MessageQueue.PrintQueue()

		//db.MessageQueue.PrintQueue()
	}

	// Controlla che in coda ci sia un messaggio di GET locale come messaggio successivo che può essere processato
	// Essendo un evento interno al processo se è in testa alla coda sono sicuro che tutti gli eventi precedenti in ordine di programma sono stati eseguiti (perché lo precedevano nella coda)
	// Questo permette di garantire che la GET venga processata anche quando non c'è un messaggio di REQUEST o ACK successivo
	resultMessage = db.MessageQueue.PopGetMessage()
	for resultMessage != nil {
		// esegue l'operazione di GET richiesta
		db.DbStore.getEntry(resultMessage.Key)
		fmt.Printf("\nGET: %s\n", resultMessage.Key)
		resultMessage = db.MessageQueue.PopGetMessage()
	}

}

func simulateDelay() {
	time.Sleep(time.Duration(500+rand.Intn(2000)) * time.Millisecond) // ritardo tra 500ms e 2s
}
