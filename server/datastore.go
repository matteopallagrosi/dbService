package main

import (
	"dbService/utils"
	"fmt"
	"sync"
)

// DataStore definisce il servizio messo a disposizione del client.
// La consistenza può essere sequenziale o causale.
type DataStore interface {
	// Get recupera il valore corrispondente a una chiave
	Get(args utils.Args, result *utils.Result) error

	// Put inserisce una nuova coppia key-value, o aggiorna il valore corrente se la chiave già esiste
	Put(args utils.Args, result *utils.Result) error

	// Delete rimuove la entry corrispondente a una data chiave
	Delete(args utils.Args, result *utils.Result) error
}

type DbStore struct {
	Store map[string]string // Store di coppie chiave-valore
	mutex sync.Mutex
}

// putEntry ritorna il valore associato alla chiave indicata, oppure una stringa che indica l'assenza della chiave nello store
func (db *DbStore) getEntry(key string) string {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	value, exist := db.Store[key]
	if !exist {
		if ConsistencyType == "SEQUENTIAL" {
			fmt.Printf("GET key %s value NOT FOUND\n", key)
		}
		return "NOT FOUND"

	} else {
		fmt.Printf("GET key %s value %s\n", key, value)
		return value
	}
}

// putEntry inserisce una nuova entry nello store key-value.
// Se esiste già una entry nello store associata alla chiave data, il valore corrispondente viene aggiornato.
func (db *DbStore) putEntry(key string, value string) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.Store[key] = value
	fmt.Printf("PUT key %s value %s\n", key, value)
}

// deleteEntry rimuove la entry associata a una data chiave nello store key-value.
// Se la chiave non esiste la delete non esegue alcuna operazione
func (db *DbStore) deleteEntry(key string) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	delete(db.Store, key)
	fmt.Printf("DELETE key %s\n", key)
}

// printDbStore stampa il contenuto del DbStore
func (db *DbStore) printDbStore() {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// Verifica se lo store è vuoto
	if len(db.Store) == 0 {
		fmt.Println("The store is empty.")
		return
	}

	// Definisce la larghezza delle colonne
	keyHeader := "Key"
	valueHeader := "Value"
	maxKeyLength := len(keyHeader)
	maxValueLength := len(valueHeader)

	// Trova la lunghezza massima di key e value
	for key, value := range db.Store {
		if len(key) > maxKeyLength {
			maxKeyLength = len(key)
		}
		if len(value) > maxValueLength {
			maxValueLength = len(value)
		}
	}

	// Stampa l'intestazione
	fmt.Printf("+-%s-+-%s-+\n", generateLine(maxKeyLength), generateLine(maxValueLength))
	fmt.Printf("| %-*s | %-*s |\n", maxKeyLength, keyHeader, maxValueLength, valueHeader)
	fmt.Printf("+-%s-+-%s-+\n", generateLine(maxKeyLength), generateLine(maxValueLength))

	// Stampa ogni entry dello store
	for key, value := range db.Store {
		fmt.Printf("| %-*s | %-*s |\n", maxKeyLength, key, maxValueLength, value)
		fmt.Printf("+-%s-+-%s-+\n", generateLine(maxKeyLength), generateLine(maxValueLength))
	}
}

// generateLine genera una riga di trattini della lunghezza desiderata
func generateLine(length int) string {
	// Crea un slice di byte di lunghezza specificata, ogni elemento è '-'
	line := make([]byte, length)
	for i := range line {
		line[i] = '-'
	}
	return string(line)
}
