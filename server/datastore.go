package main

import (
	"dbService/utils"
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
		return "La chiave cercata non esiste"
	} else {
		return value
	}
}

// putEntry inserisce una nuova entry nello store key-value.
// Se esiste già una entry nello store associata alla chiave data, il valore corrispondente viene aggiornato.
func (db *DbStore) putEntry(key string, value string) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.Store[key] = value
}

// deleteEntry rimuove la entry associata a una data chiave nello store key-value.
// Se la chiave non esiste la delete non esegue alcuna operazione
func (db *DbStore) deleteEntry(key string) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	delete(db.Store, key)
}
