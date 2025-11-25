# cockpit-aftermarket

ETL process for a PowerBI-based cockpit that must show many different data types. First draft uses Python to query for data through API to save .parquet file to be read by .duckdb. The focus is to create a light and shareable process that must be robust to avoid any manual data normalization.



\## Cose da sapere



Il report aftermarket è un report che viene creato mensilmente e racchiude al suo interno un gran numero di dati molto diversi tra di loro, con numerose fonti diversificate. Questo ha rappresentato un problema non indifferente per la normalizzazione del dato prima dell'immissione nel report scritto in PowerBI. Il problema ha trovato la sua soluzione in DuckDB, una pratica libreria Python capace di creare database locali portatili e facili da setuppare e spostare. Soprattutto, sono single-file, e quindi non richiedono laboriosi lavori di attivazione di server SQL. Il database, già creato, non necessita di modifiche strutturali (a meno che non si voglia aggiungere qualcosa). Il fulcro del programma è ##data\_etl.py##, che si occupa di prendere i dati dalle varie fonti, manipolarli, prepararli per il caricamento e infine caricarli, aggiungendo un importante flag latest che traccia l'ultima disponibilità del dato con uno specifico ID. Fondamentali sono anche le librerie utilizzate: pandas (ahimè) per la pulizia e normalizzazione, pyistat per recuperare i dati dall'ISTAT, ecbdata per recuperare i dati dalla European Central Bank, imfp per recuperare i dati dal Fondo Monetario Internazionale, sqlalchemy per recuperare i dati dal nostro database SQL Azure in cloud, datetime per la gestione delle date e infine utilities per la funzionalità di login al server.



\## Come generare il report



Come si genera un corretto report per l'aftermarket usando data\_etl.py? Prima di tutto è necessario aprire il file .py con un IDE, come Spyder o Jupyter Notebook. Una volta aperto, bisogna assicurarsi che nel proprio ambiente Python siano presenti tutte le librerie necessarie al funzionamento dello script. Queste librerie sono nel file requirements.txt. Per installare una libreria bisogna digitare, nella cosiddetta "console", `pip install {libreria}`. Qualora non sia già installata, il programma pip installerà la libreria corrispondente aggiornandola all'ultima versione.



Dopo aver preparato il proprio ambiente Python, è il momento di fare l'unica azione manuale necessaria per il report. Tutti i dati del report si trovano in questa cartella condivisa: L:\\01.Dati\\04.Varie\\08.Cockpit. Il file che ci interessa è L:\\01.Dati\\04.Varie\\08.Cockpit\\files\\Drewry\_WCI.xlsx. Per completare questo file, è necessario andare sul sito ufficiale Drewry e compilare manualmente i campi ispezionando col mouse ciascun "nodo" del grafico che si trova qui: ##https://www.drewry.co.uk/supply-chain-advisors/supply-chain-expertise/world-container-index-assessed-by-drewry##. Trascinare in basso le formule ed ecco fatto, il file è pronto. Salvare e chiudere.



Assicurarsi che il file Excel dell'IAM sia pronto e caricato qui: ##L:\\01.Dati\\04.Varie\\08.Cockpit\\csv\_database\\IAM##.



Dopo aver eseguito queste operazioni, è il momento di ritornare dal nostro codice data\_etl.py. Aprire il codice, e avviarlo. Il codice farà tutto da solo fino alla fine, e non dovrebbe lanciare alcun errore. Il report si trova qui: ##L:\\01.Dati\\04.Varie\\08.Cockpit\\Report\_cockpit##. Basterà aprirlo e aggiornarlo. Va solo cambiata l'introduzione, e la parte del Drewry che può essere semplicemente tradotta da quella ufficiale. Fatto!



\## Specifiche tecniche



A livello tecnico, il codice più importante, ovvero data\_etl.py, è strutturato in 6 macrosezioni.



1\. La prima parte è quella del recupero dei dati. Qui troviamo la query di estrazione dal nostro database in cloud, e utilizziamo le librerie pyistat, ecbdata e imfp per ottenere i dati dalle API dei servizi online. Fare particolare attenzione alle liste qui presenti, tipo quella delle materie prime, che viene utilizzata come filtro nel dataframe che viene estratto dalla libreria imfp del Fondo Monetario Internazionale.



2\. La parte due riguarda la codificazione delle variabili. Il database in duckdb ha degli indici, che vengono mappati in tutte questi dizionari presenti in questa fase. La mappatura la eseguo partendo da questi dizionari, e verrà ripresa dalle varie manipolazioni dei dataframe. Lo scopo è ottenere dei dataframe che siano pronti per essere inseriti in duckdb senza problemi, perciò è ##FONDAMENTALE## che in caso di bisogno questi dizionari vengano aggiornati e ##INSERITI## nel database duckdb, altrimenti l'insert fallirà miseramente. Perciò, fare attenzione quando bisogna aggiungere dati, perché vanno inseriti e mappati nel database duckdb, nella tabella dimension\_Evento.



3\. La parte tre entra nel vivo della pulizia del dato. Ottenendo dati molto diversificati è utile una procedura di pulizia dei dati, che in questo caso funziona in base alla fonte del dataframe. Inoltre sono presenti anche le funzioni che tirano fuori il dato dell'IAM dai file Excel. Tutte queste funzioni ritornano un df, e vengono chiamate assegnando ai df non puliti il nome m\_{dataframe\_iniziale}. Tra questi c'è anche l'estrazione del dato dal cloud SQL.



4\. La parte quattro prepara i dataframe per l'inserzione, eliminando le colonne inutili e, ove necessario, applicando una pivoting attraverso la funzionalità .melt di pandas. Questo serve perché la divisione del database è piuttosto rigida e richiede anno, mese, id evento, ecc... Quando vengono chiamati, si assegna manualmente l'evento creando una colonna con quello stesso valore costante in ogni riga del dataframe, da cui la funzione preleverà il rispettivo ID dai precedenti dizionari di codifica.



5\. In questa parte, tutti i dataframe vengono inseriti in delle liste. La logica per i dataframe IAM e non-IAM è differente e perciò si creano due liste. Il sunto però è lo stesso: i dati vengono prelevati dalle righe dei dataframe, e inseriti in base all'idDato, idData e al valore. Per l'IAM teniamo in considerazione anche l'idTipoVeicolo. Infine, si applica il succitato flag latest per trovare qual è l'ultimo dato presente per ciascun idDato. Per il caricamento dei dati immatricolativi è tutto più facile perché sono già estratti in formato colonnare, ma l'applicazione di latest è più complesso dato che ci sono più "unicità" in quel dataframe. Da studiare bene questa logica per comprenderla appieno.



6\. Infine, nella parte finale del codice, creiamo dei CSV da alcune viste preimpostate nel database duckdb, le esportiamo sotto forma di CSV (abilitando così un aggiornamento senza problemi via PowerBI, cosa che con i file in .parquet non era possibile) e chiudiamo la connessione.



Il report PowerBI in se non è particolarmente complesso, dato che le tabelle che vengono importate al suo interno sono molto schematiche. Questo fa in modo che non ci sia bisogno di grandi misure DAX. In particolare ci sono delle misure per bloccare la visione del dato all'ultimo quarter o che mostrano il cumulato, per le immatricolazioni (che sono JOINate a una tabella che, oltre a tracciare il mercato specifico, traccia anche se sono veicoli commerciali o no).



Le misure specifiche invece determinano ad esempio il cumulato per dati di vario tipo, le variazioni mese su mese e anno su anno, e cose così. Sono comunque formule semplici e di facile debug.

