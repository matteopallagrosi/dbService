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
  echo "Starting server replica with index $i..."

  # Lancia ogni replica in background con l'indice passato da riga di comando
  go run ./server $i &

done

echo "Started $NUM_REPLICAS replicas."