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
	BaseName         string
	Container        bool          // Questa variabile distingue tra l'esecuzione con Docker o senza
	TimeoutDuration  time.Duration // Durata del timeout di inattività
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
	BaseName = os.Getenv("BASE_NAME")
	Timeout, err := strconv.Atoi(os.Getenv("TIMEOUT"))
	TimeoutDuration = time.Duration(Timeout) * time.Second
	ConsistencyType = os.Getenv("CONSISTENCY_TYPE")
	if os.Getenv("CONTAINER") == "YES" {
		Container = true
	} else {
		Container = false
	}
}

func main() {
	// Recupera da riga di comando l'indice da assegnare al server lanciato
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

	//Crea un'istanza di struct che implementa l'interfaccia DataStore
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
			Address:            GetServerAddress(serverIndex),
			Addresses:          []utils.ServerAddress{},
			AddressToClient:    GetServerAddressToClient(serverIndex),
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

		//Configura gli indirizzi delle altre repliche del db
		for i := 0; i < NumReplicas; i++ {
			if i != serverIndex {
				newAddress := GetServerAddress(i)
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
			Address:            GetServerAddress(serverIndex),
			Addresses:          []utils.ServerAddress{},
			AddressToClient:    GetServerAddressToClient(serverIndex),
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

		//Configura gli indirizzi delle altre repliche del db
		for i := 0; i < NumReplicas; i++ {
			if i != serverIndex {
				newAddress := GetServerAddress(i)
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
					fmt.Println("Error while accepting connection:", err)
					continue
				}
				resetTimer()
				go dbSequential.handleConnection(conn)
			}
		}(listener)

		// Registra un nuovo server RPC
		server := rpc.NewServer()
		err = server.RegisterName("Datastore", dataStore)
		if err != nil {
			log.Fatal("Format of service datastore is not correct: ", err)
		}

		// Si mette in ascolto su una specifica porta
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

		// Permette al server di accettare richieste di connessione sul Listener e serve queste richieste
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
					fmt.Println("Error while accepting connection:", err)
					continue
				}
				resetTimer()
				go dbCausal.handleConnection(conn)
			}
		}(listener)

		// Registra un nuovo server RPC
		server := rpc.NewServer()
		err = server.RegisterName("Datastore", dataStore)
		if err != nil {
			log.Fatal("Format of service datastore is not correct: ", err)
		}

		// Si mette in ascolto su una specifica porta
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

		// Permette al server di accettare richieste di connessione sul Listener e serve queste richieste
		server.Accept(rpcListener)
	}
}

func GetServerAddress(serverIndex int) utils.ServerAddress {
	if Container {
		return utils.ServerAddress{IP: BaseName + "-" + strconv.Itoa(serverIndex), Port: strconv.Itoa(BasePort)}
	} else {
		return utils.ServerAddress{IP: "localhost", Port: strconv.Itoa(BasePort + serverIndex)}
	}
}

func GetServerAddressToClient(serverIndex int) utils.ServerAddress {
	if Container {
		return utils.ServerAddress{IP: BaseName + "-" + strconv.Itoa(serverIndex), Port: strconv.Itoa(BasePortToClient)}
	} else {
		return utils.ServerAddress{IP: "localhost", Port: strconv.Itoa(BasePortToClient + serverIndex)}
	}
}
