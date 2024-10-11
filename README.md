# Store key-value con garanzie di consistenza sequenziale o causale
Il sistema realizza l'implementazione di uno store key-value, con cui è possibile interagire attraverso le operazioni di PUT, GET e DELETE.
Inoltre fornisce garanzie di consistenza sequenziale o causale, in base al valore configurato per la variabile d'ambiente `CONSISTENCY_TYPE`.
Il numero di repliche dello store lanciate può essere configurato tramite la variabile `NUM_REPLICAS`.
Il sistema può essere eseguito localmente, in una versione standalone oppure tramite soluzione con container con Docker Compose, ed è prevista anche l'esecuzione su istanze AWS EC2, come riportato di seguito.

### Istruzioni per esecuzione in locale
Il sistema può essere lanciato localmente in ambienti Linux senza utilizzo dei container, settando `CONTAINER = NO`, tramite lo script `start_servers.sh`.
Questo script lancia `NUM_REPLICAS` repliche dello store, che garantiscono una specifica tipologia di consistenza, secondo quanto definito nel file `.env`.
Ogni replica visualizza l'output delle richieste che gli vengono inoltrate su un differente terminale. Inoltre dopo il lancio dei server, viene eseguito un client, con cui è possibile interagire da riga di comando per richiedere specifiche operazioni sullo store.

Per eseguire il sistema con Docker Compose è sufficiente impostare `CONTAINER = YES` nel file `.env` e lanciare il comando `docker compose up` nella directory in cui è presente il file `docker-compose.yml`.
In questo modo viene istanziato un container per ogni replica dello store.


### Istruzioni per esecuzione su istanza EC2
Il sistema può essere eseguito su un istanza di AWS EC2. 
La configurazione presentata prevede l'utilizzo di un'immagine di Sistema Operativo AMI Amazon Linux.
Dopo aver creato l'istanza EC2, è possibile connettersi a essa utilizzando il comando `ssh -i <private-key>.pem ec2-user@<VM-Public-IPv4>`, indicando il path del file .pem contenente la chiave privata generata durante la creazione dell'istanza e l'indirizzo IPv4 pubblico con cui questa è raggiungibile, recuperabile dall'interfaccia di gestione dell'istanza EC2 su AWS.
Una volta connessi all'istanza EC2, per eseguire il sistema sono necessari i seguenti comandi:

1. `sudo yum install docker -y` per installare docker

2. `sudo curl -L https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose` per installare la versione più recente di Docker Compose

3. `sudo chmod +x /usr/local/bin/docker-compose` in modo da renderlo eseguibile

4. `docker-compose version` per verificare il successo dell'installazione

5. `sudo yum install git -y` per installare Git

6. `git clone https://github.com/matteopallagrosi/dbService.git` per clonare la repository contenente il sistema

7. `cd dbService` per collocarsi nella directory principale del sistema

8. `sudo service docker start` per avviare il Docker deamon

9. `sudo docker-compose up --build` per lanciare il sistema, che esegue il test secondo quanto configurato.

### Variabili d'ambiente
Nel file `.env` sono contenute tutte le variabili d'ambiente configurabili per modificare il comportamento del sistema.
Tali variabili sono indicate di seguito nel dettaglio:

- `NUM_REPLICAS`: numero di repliche dello store. I test sono configurati per operare con quattro repliche, così da poterne gestire l'input e verificare la correttezza dell'output ottenuto. Lo store però può operare con un numero qualunque di repliche. 
- `BASE_PORT`: porta che ogni replica utilizza per la comunicazione con le altre repliche dello store.
- `BASE_PORT_TO_CLIENT`: porta esposta ai client per ricevere richieste di GET, PUT o DELETE.
- `BASE_NAME`: nome base di ogni replica, che una volta istanziata assume come nome `BASE_NAME-<index>`, con index che assume valore univoco tra 0 e NUM_REPLICAS. BASE_NAME deve essere consistente con il nome scelto per i container nel docker compose.
- `TIMEOUT`: intervallo di tempo di inattività oltre il quale viene effettuato lo shutdown delle repliche, in assenza di messaggi propagati. Ogni volta che una replica deve processare qualche messaggio, il timer viene resettato. Utilizzato per terminare le repliche una volta completati i test.
- `CONSISTENCY_TYPE`: tipologia di consistenza da garantire nell'interazione con le repliche dello store, può assumere valore `SEQUENTIAL` o `CAUSAL`.
- `TEST`: tipologia di test da eseguire. Ciascun tipo di consistenza può essere testato con un test `SIMPLE` oppure `COMPLEX`.
- `CONTAINER`: utilizzo dei container in caso di `YES`, oppure esecuzione in locale se pari a `NO`.