#!/bin/bash

# Carica le variabili dal file .env
if [ -f .env ]; then
   export $(grep -v '^#' .env | xargs)
fi


# Assicurati che la variabile NUM_REPLICAS sia presente nel file .env
if [ -z "$NUM_REPLICAS" ]; then
  echo "NUM_REPLICAS not specified in the .env file"
  exit 1
fi

# Avviare le repliche in base al valore di NUM_REPLICAS
for ((i=0; i<NUM_REPLICAS; i++)); do
  echo "Starting server replica with index $i in a new terminal..."

  # Lancia ogni replica in una nuova finestra del terminale
  gnome-terminal -- bash -c "go run ./server $i; exec bash" &

done

echo "Started $NUM_REPLICAS replicas."

# Attende 5 secondi per garantire che tutti i server siano attivi
echo "Waiting for servers to be ready..."
sleep 5

# Avvia il client in modalità interattiva dopo aver avviato i server
echo "Starting client..."
go run ./client