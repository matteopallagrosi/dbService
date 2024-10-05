package main

import (
	"dbService/utils"
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	NumReplicas      int
	BasePort         int
	BasePortToClient int
	ConsistencyType  string
	TimeoutDuration  = 20 * time.Second // Durata del timeout di inattività
)

func init() {
	// Carica le variabili d'ambiente dal file .env
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Recupera e converte le variabili d'ambiente
	NumReplicas, _ = strconv.Atoi(os.Getenv("NUM_REPLICAS"))
	BasePort, _ = strconv.Atoi(os.Getenv("BASE_PORT"))
	BasePortToClient, _ = strconv.Atoi(os.Getenv("BASE_PORT_TO_CLIENT"))
	ConsistencyType = os.Getenv("CONSISTENCY_TYPE")
}

func main() {

	// recupera da riga di comando l'indice da assegnare al server lanciato
	if len(os.Args) < 2 {
		fmt.Println("Please provide an index as an argument.")
		return
	}

	// Recupera l'indice passato da riga di comando
	indexStr := os.Args[1]
	serverIndex, err := strconv.Atoi(indexStr)
	if err != nil {
		fmt.Println("Invalid index:", indexStr)
		return
	}

	fmt.Printf("Starting server instance with index: %d\n", serverIndex)

	//Create an instance of struct which implements dataStore interface
	var dataStore DataStore
	if ConsistencyType == "SEQUENTIAL" {
		// crea un server con garanzie di consistenza sequenziale
		dbSequential := &DbSequential{
			ID: serverIndex,
			DbStore: DbStore{
				Store: make(map[string]string),
				mutex: sync.Mutex{},
			},
			MessageQueue: utils.MessageQueue{},
			Clock: Clock{
				value: 0,
				mutex: sync.Mutex{},
			},
			Address: utils.ServerAddress{
				IP:   "localhost",
				Port: strconv.Itoa(BasePort + serverIndex),
			},
			Addresses: []utils.ServerAddress{},
			AddressToClient: utils.ServerAddress{
				IP:   "localhost",
				Port: strconv.Itoa(BasePortToClient + serverIndex),
			},
			FIFOQueues:         make(map[int]*utils.MessageQueue),
			ExpectedNextSeqNum: make(map[int]*NextSeqNum),
			NextSeqNum: NextSeqNum{
				SeqNum: 0,
				mutex:  sync.Mutex{},
			},
		}

		for i := 0; i < NumReplicas; i++ {
			if i != serverIndex {
				dbSequential.FIFOQueues[i] = &utils.MessageQueue{}
				dbSequential.ExpectedNextSeqNum[i] = &NextSeqNum{
					SeqNum: 0,
					mutex:  sync.Mutex{},
				}
			}
		}

		//Configuro gli indirizzi delle altre repliche del db
		for i := 0; i < NumReplicas; i++ {
			if i != serverIndex {
				newAddress := utils.ServerAddress{IP: "localhost", Port: strconv.Itoa(BasePort + i)}
				dbSequential.Addresses = append(dbSequential.Addresses, newAddress)
			}
		}

		dataStore = dbSequential

	} else if ConsistencyType == "CAUSAL" {
		// crea un server con garanzie di consistenza causale
		dbCausal := &DbCausal{
			ID: serverIndex,
			DbStore: DbStore{
				Store: make(map[string]string),
				mutex: sync.Mutex{},
			},
			MessageQueue: utils.VectorMessageQueue{},
			Clock: VectorClock{
				value: make([]int, NumReplicas),
				mutex: sync.Mutex{},
			},
			Address: utils.ServerAddress{
				IP:   "localhost",
				Port: strconv.Itoa(BasePort + serverIndex),
			},
			Addresses: []utils.ServerAddress{},
			AddressToClient: utils.ServerAddress{
				IP:   "localhost",
				Port: strconv.Itoa(BasePortToClient + serverIndex),
			},
			FIFOQueues:         make(map[int]*utils.VectorMessageQueue),
			ExpectedNextSeqNum: make(map[int]*NextSeqNum),
			NextSeqNum: NextSeqNum{
				SeqNum: 0,
				mutex:  sync.Mutex{},
			},
		}

		for i := 0; i < NumReplicas; i++ {
			if i != serverIndex {
				dbCausal.FIFOQueues[i] = &utils.VectorMessageQueue{}
				dbCausal.ExpectedNextSeqNum[i] = &NextSeqNum{
					SeqNum: 0,
					mutex:  sync.Mutex{},
				}
			}
		}

		//Configuro gli indirizzi delle altre repliche del db
		for i := 0; i < NumReplicas; i++ {
			if i != serverIndex {
				newAddress := utils.ServerAddress{IP: "localhost", Port: strconv.Itoa(BasePort + i)}
				dbCausal.Addresses = append(dbCausal.Addresses, newAddress)
			}
		}
		dataStore = dbCausal

	} else {
		log.Fatal("Invalid CONSISTENCY_TYPE in .env. It must be SEQUENTIAL or CAUSAL.")
	}

	startRPCServer(dataStore)
}

// startRPCServer avvia il server RPC, che potrà quindi essere contattato dai client
func startRPCServer(dataStore DataStore) {
	// Inizializza il timer di inattività
	timer := time.NewTimer(TimeoutDuration)
	resetTimer := func() {
		if !timer.Stop() {
			<-timer.C
		}
		timer.Reset(TimeoutDuration)
	}

	// Goroutine che ascolta lo scadere del timer
	go func() {
		<-timer.C
		fmt.Println("Timeout reached, shutting down server...")

		// Stampa il contenuto del datastore prima di terminare
		if db, ok := dataStore.(*DbSequential); ok {
			db.DbStore.printDbStore()
		} else if db, ok := dataStore.(*DbCausal); ok {
			db.DbStore.printDbStore()
		}

		os.Exit(0)
	}()

	if dbSequential, ok := dataStore.(*DbSequential); ok {
		// Ogni replica si mette in ascolto di update da parte delle altre repliche
		listener, err := net.Listen("tcp", dbSequential.Address.GetFullAddress())
		if err != nil {
			fmt.Println("Error while starting server: ", err)
			return
		}
		log.Printf("Replica listens on port %s", dbSequential.Address.Port)

		// Gestisce richieste di update o ack da parte delle altre repliche su una goroutine
		go func(listener net.Listener) {
			for {
				// Accetta la connessione
				conn, err := listener.Accept()
				if err != nil {
					fmt.Println("Errore nell'accettare la connessione:", err)
					continue
				}
				resetTimer()
				go dbSequential.handleConnection(conn)
			}
		}(listener)

		// Register a new rpc server and the struct we created above.
		// Only structs which implement datastore interface
		// are allowed to register themselves
		server := rpc.NewServer()
		err = server.RegisterName("Datastore", dataStore)
		if err != nil {
			log.Fatal("Format of service datastore is not correct: ", err)
		}

		// Listen for incoming TCP packets on specified port
		rpcListener, err := net.Listen("tcp", dbSequential.AddressToClient.GetFullAddress())
		if err != nil {
			log.Fatal("Error while starting RPC server:", err)
		}
		log.Printf("RPC server listens on port %s", dbSequential.AddressToClient.Port)

		defer func(listener net.Listener) {
			err := listener.Close()
			if err != nil {
				log.Fatal("Error while closing RPC server:", err)
			}
		}(rpcListener)

		// Allow RPC server to accept requests connections on the listener
		// and serve requests for each incoming connection.
		server.Accept(rpcListener)

	} else if dbCausal, ok := dataStore.(*DbCausal); ok {
		// Ogni replica si mette in ascolto di update da parte delle altre repliche
		listener, err := net.Listen("tcp", dbCausal.Address.GetFullAddress())
		if err != nil {
			fmt.Println("Error while starting server: ", err)
			return
		}
		log.Printf("Replica listens on port %s", dbCausal.Address.Port)

		// Gestisce richieste di update o ack da parte delle altre repliche su una goroutine
		go func(listener net.Listener) {
			for {
				// Accetta la connessione
				conn, err := listener.Accept()
				if err != nil {
					fmt.Println("Errore nell'accettare la connessione:", err)
					continue
				}
				resetTimer()
				go dbCausal.handleConnection(conn)
			}
		}(listener)

		// Register a new rpc server and the struct we created above.
		// Only structs which implement datastore interface
		// are allowed to register themselves
		server := rpc.NewServer()
		err = server.RegisterName("Datastore", dataStore)
		if err != nil {
			log.Fatal("Format of service datastore is not correct: ", err)
		}

		// Listen for incoming TCP packets on specified port
		rpcListener, err := net.Listen("tcp", dbCausal.AddressToClient.GetFullAddress())
		if err != nil {
			log.Fatal("Error while starting RPC server:", err)
		}
		log.Printf("RPC server listens on port %s", dbCausal.AddressToClient.Port)

		defer func(listener net.Listener) {
			err := listener.Close()
			if err != nil {
				log.Fatal("Error while closing RPC server:", err)
			}
		}(rpcListener)

		// Allow RPC server to accept requests connections on the listener
		// and serve requests for each incoming connection.
		server.Accept(rpcListener)
	}
}
