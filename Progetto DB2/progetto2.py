import time
import InserimentoDati
import csv
from pymongo import MongoClient
from neo4j import GraphDatabase
from Database import Mongo_Connect, driver_neo4j, chiusura_Driver_neo4j
import subprocess
import os
import glob
import pandas

def pulisci_cartella():
    #cancella i grafici
    for file_csv in glob.glob("*.csv"):
        try:
            os.remove(file_csv)
            print(f"Rimosso file CSV: {file_csv}")
        except Exception as e:
            print(f"Errore rimuovendo {file_csv}: {e}")
    
    #cancella i file ong
    for file_png in glob.glob("*.png"):
        try:
            os.remove(file_png)
            print(f"Rimosso file PNG: {file_png}")
        except Exception as e:
            print(f"Errore rimuovendo {file_png}: {e}")


def esegui_query_neo4j(driver, query):
    #restituisce il tempo in ms e i record
    with driver.session() as session:
        inizio = time.perf_counter()
        risultati = session.run(query)
        records = list(risultati)
        fine = time.perf_counter()
    elapsed_ms = (fine - inizio) * 1000
    return elapsed_ms, records

#funzione per eseguire la query mongo
def esegui_query_mongodb(db, collezione, pipeline):
    inizio = time.perf_counter()
    risultati = list(db[collezione].aggregate(pipeline))
    fine = time.perf_counter()

    elapsed_ms = (fine - inizio) * 1000
    return elapsed_ms, risultati


def main(numero_aziende_totali,numero_aziende_italiane,numero_transazioni,numero_prodotti):
    pulisci_cartella()
    print("Avvio software Progetto Database NoSQL")
    #input("Premi Invio quando sei pronto per popolare il database...")
    InserimentoDati.popola_Neo4j_MongoDB(numero_aziende_totali,numero_aziende_italiane,numero_transazioni,numero_prodotti)
    print("Database riempiti\nFase di esecuzione query\n")

    #preparazione driver e CSV
    #lista delle query
    #la prima query ritorna tutti gli id_azienda
    #la seconda query ritorna gli id azienda e le loro transazioni
    #la terza query ordina in modo decrescente le transazioni in base al campo importo_totale e ritona id_azineda,Id_prodotto e importoTotale
    #la quarta query è come la terza ma ritorna anche l'Iva Addebitata di tutte le transazioni con importo totale maggiore di 500
    #la quinta query cerca tutte le aziende e, per ognuna, verifica se ha effettuato delle transazioni. 
    #se sì, ne conta il numero e somma gli importi totali. Se un’azienda non ha transazioni, viene comunque considerata, ma con zero transazioni e zero importo.
    querys = ["""
        MATCH (a:Azienda)
        RETURN a.id_azienda AS Azienda_ID
        """,
        """
        MATCH (a:Azienda)-[:EFFETTUA]->(t:Transazione)
        RETURN a.nomeAzienda AS nome_azienda, t.id_transazione AS id_transazione
        """,
        """
        MATCH (a:Azienda)-[:EFFETTUA]->(t:Transazione)-[:CONTIENE]->(p:Prodotto)
        RETURN 
            a.id_azienda AS id_azienda, 
            p.id_prodotto AS id_prodotto, 
            t.importo_totale AS importo
        ORDER BY t.importo_totale DESC
        """,
        """
        MATCH (a:Azienda)-[:EFFETTUA]->(t:Transazione)-[:CONTIENE]->(p:Prodotto)
        WHERE t.importo_totale > 500
        AND t.IVA_addebitata IS NOT NULL
        RETURN 
            a.id_azienda AS id_azienda, 
            p.id_prodotto AS id_prodotto, 
            t.importo_totale AS importo,
            t.IVA_addebitata AS iva
        ORDER BY t.importo_totale DESC
        """,
        """
        OPTIONAL MATCH (a:Azienda)-[:EFFETTUA]->(t:Transazione)-[:CONTIENE]->(p:Prodotto)-[:APPARTIENE]->(c:Categoria)
        WITH 
            a.id_azienda AS id_azienda,
            c.nome_categoria AS nome_categoria,
            COUNT(DISTINCT t) AS num_transazioni,
            SUM(t.importo_totale) AS totale_importi,
            SUM(p.prezzo_unitario * (c.IVA / 100.0)) AS totale_iva
        RETURN 
            id_azienda,
            nome_categoria,
            num_transazioni,
            totale_importi,
            totale_iva
        ORDER BY 
            totale_importi DESC
        """   
        ]
    
    for i,query in enumerate(querys):
        nome_file = f"tempi_Query_Neo4j_{i}.csv"  #modifico il nome del file csv ad ogni iterazione
        with open(nome_file, "w", newline="") as csvfile:#apriamo il file csv in sola scrittura
            writer = csv.writer(csvfile) 
            writer.writerow(["Nome_query", "run_id", "tempo_ms"]) #diamo i nomi di intestazioni delle colonne

            #prima esecuzione vergine (run_id = 0)
            neo4j_driver = driver_neo4j()   #apro connessione

            elapsed, records = esegui_query_neo4j(neo4j_driver, query) #esegue la query

            neo4j_driver.close()#chiudo connessione

            writer.writerow([f"Query{i}", 0, elapsed]) #mette i nomi delle intestazioni
            print("___Neo4j Prima Esecuzione della query___\n")
            print(f"Run 0: {elapsed:.2f} ms, {len(records)} record trovati") #stampa il tempo in ms con due cifre decimali e dice il numero di record trovati
            print("Valori Trovati:\n")

            #with open(f"Record Trovati {i}.csv", "w", newline="") as RecordTrovati:

                #writer2=csv.writer(RecordTrovati)
                #writer2.writerow(["Chiave", "valore"])
                #for record in records:
                #   for key, value in record.items(): #stampiamo i vari campi dei record trovati
                #        print(f"{key}: {value}")
                #        writer2.writerow([key,value]) #salviamo i record trovati dentro un file
                #    print("---")

            #esecuzioni da 1 a 30
            neo4j_driver = driver_neo4j()
            media_tempo=0
            tempi=[]
            for run_id in range(1, 32):
                elapsed, records = esegui_query_neo4j(neo4j_driver, query)
                writer.writerow(["Query", run_id, elapsed])
                if run_id!=1:  #dobbiamo escludero la prima esecuzione nel calcolo dei tempi
                    tempi.append(elapsed) #aggiungiamo il tempo di ogni esecuzione
                    print(f"Run {run_id}: elapsed={elapsed}, tempi count={len(tempi)}")
                if(run_id==1):
                    print(f"___Neo4j Esecuzione della Query {i}___")
                    print(f"Run {run_id}: {elapsed:.2f} ms, {len(records)} record trovati")
            if run_id != 1:
                for tempo in tempi:
                    media_tempo=tempo+media_tempo
            media_tempo=media_tempo/len(tempi)
            writer.writerow(["Media", "-", media_tempo])
            with open(f"Tempi_Di_Esecuzione_30_query_Neo4j", "a", newline="") as file_tempi:#apriamo il file csv in sola scrittura
                writer = csv.writer(file_tempi) 
                writer.writerow([f"Query{i}_Neo4j",]) #diamo i nomi di intestazioni delle colonne
                writer.writerows([[tempo] for tempo in tempi])
            neo4j_driver.close()

    



    #parte di mongoDB
    #creiamo le query per mongoDB
    pipelines = [
            [
                { "$project": { "_id": 0, "id_azienda": 1 } }
            ],

            [
                {
                    "$lookup": {
                        "from": "transazioni",
                        "localField": "id_azienda",
                        "foreignField": "id_venditore",
                        "as": "transazioni"
                    }
                },
                { "$unwind": "$transazioni" },
                {
                    "$project": {
                        "_id": 0,
                        "nome_azienda": "$nomeAzienda",
                        "id_transazione": "$transazioni.id_transazione"
                    }
                }
             ],
            [
                    { 
                        "$lookup": {
                            "from": "transazioni",
                            "localField": "id_azienda",
                            "foreignField": "id_venditore",  
                            "as": "transazioni"
                        }
                    },
                    { "$unwind": "$transazioni" },
                    { 
                        "$lookup": {
                            "from": "prodotti",
                            "localField": "transazioni.id_prodotto",
                            "foreignField": "id_prodotto",
                            "as": "prodotti"
                        }
                    },
                    { "$unwind": "$prodotti" },
                    { 
                        "$project": {
                            "_id": 0,
                            "id_azienda": "$id_azienda",
                            "id_prodotto": "$prodotti.id_prodotto",
                            "importo": "$transazioni.importo_totale"
                        }
                    },
                    { "$sort": { "importo": -1 } }
            ],
            #4 query
            [
                { 
                    "$lookup": {
                        "from": "transazioni",
                        "localField": "id_azienda",
                        "foreignField": "id_venditore",  
                        "as": "transazioni"
                    }
                },
                { "$unwind": "$transazioni" },
                { 
                    "$match": { 
                        "transazioni.importo_totale": { "$gt": 500 },
                        "transazioni.IVA_addebitata": { "$exists": True, "$ne": None }
                    } 
                },
                { 
                    "$lookup": {
                        "from": "prodotti",
                        "localField": "transazioni.id_prodotto",
                        "foreignField": "id_prodotto",
                        "as": "prodotti"
                    }
                },
                { "$unwind": "$prodotti" },
                { 
                    "$project": {
                        "_id": 0,
                        "id_azienda": "$id_azienda",
                        "id_prodotto": "$prodotti.id_prodotto",
                        "importo_totale": "$transazioni.importo_totale",
                        "iva_addebitata": "$transazioni.IVA_addebitata",
                    } 
                },
                { "$sort": { "importo_totale": -1 } }
            ],
            [
                {
                    "$lookup": {
                        "from": "transazioni",
                        "localField": "id_azienda",
                        "foreignField": "id_venditore",
                        "as": "transazioni"
                    }
                },
                { "$unwind": { "path": "$transazioni", "preserveNullAndEmptyArrays": True } },
                
                {
                    "$lookup": {
                        "from": "prodotti",
                        "localField": "transazioni.id_prodotto",
                        "foreignField": "id_prodotto",
                        "as": "prodotti"
                    }
                },
                { "$unwind": { "path": "$prodotti", "preserveNullAndEmptyArrays": True } },
                
                {
                    "$lookup": {
                        "from": "categorie",
                        "localField": "prodotti.id_categoria",
                        "foreignField": "id_categoria",
                        "as": "categorie"
                    }
                },
                { "$unwind": { "path": "$categorie", "preserveNullAndEmptyArrays": True } },

                {
                    "$group": {
                        "_id": {
                            "id_azienda": "$id_azienda",
                            "nome_categoria": "$categorie.nome_categoria"
                        },
                        "transazioni_distinte": { "$addToSet": "$transazioni._id" },
                        "totale_importi": { "$sum": "$transazioni.importo_totale" },
                        "totale_iva": {
                            "$sum": {
                                "$multiply": [
                                    "$prodotti.prezzo_unitario",
                                    { "$divide": ["$categorie.IVA", 100] }
                                ]
                            }
                        }
                    }
                },
                
                {
                    "$project": {
                        "_id": 0,
                        "id_azienda": "$_id.id_azienda",
                        "nome_categoria": "$_id.nome_categoria",
                        "num_transazioni": { "$size": "$transazioni_distinte" },
                        "totale_importi": 1,
                        "totale_iva": 1
                    }
                },
                { "$sort": { "totale_importi": -1 } }
]

                
            
        ]
    input("Premi per vedere la parte di MongoDB\n")
    for j, pipeline in enumerate(pipelines):
        #input("Premi per eseguire la query...")
        nome_file = f"tempi_Query_Mongo{j}.csv"
        with open(nome_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Nome_query", "run_id", "tempo_ms"])
            
            client, db = Mongo_Connect()  #connessione aperta
            collection_name = "aziende" 
            
            #prima esecuzione vergine
            elapsed, records = esegui_query_mongodb(db, collection_name, pipeline)
            client.close() #chiusura connessione
            writer.writerow([f"MongoQuery{j}", 0, elapsed])#prima riga del file cvs
            print(f"Query {j}")
            print(f"Run 0: {elapsed:.2f} ms, {len(records)} record trovati") #stampiamo i dati della prima esecuzione e il numero dei record trovati
            for record in records: #iteriamo sui record trovati
                for chiave, valore in record.items():#record.items restituisce la lista chiave-valore di record che è un dizionario
                    print(f"{chiave}: {valore}") #stampiamo chiave-valore
                print("---")
            client,db=Mongo_Connect()

            print(f"Stiamo entrando nel ciclo delle 30 esecuzioni mongoDB, valore j : {j}")
            tempi=[]
            media_tempo=0
            for run_id in range(1, 31): #facciamo le rimanenti esecuzioni
                elapsed, records = esegui_query_mongodb(db, collection_name, pipeline) #richiamiamo la funzione di esecuzione delle query mongo
                writer.writerow([f"MongoQuery{run_id}", run_id, elapsed])#per ogni riga abbiamo il numero della query eseguita, l'indice di esecuzione e il tempo di exe
                tempi.append(elapsed)
                if(run_id==1):
                    print(f"Run {run_id}: {elapsed:.2f} ms, {len(records)} record trovati")
            for tempo in tempi:
                 media_tempo=tempo+media_tempo
            media_tempo=media_tempo/len(tempi)
            writer.writerow(["Media", "-", media_tempo])
            with open(f"Tempi_Di_Esecuzione_30_query_MongoDB", "a", newline="") as file_tempi_Mongo:#apriamo il file csv in sola scrittura
                writer = csv.writer(file_tempi_Mongo) 
                writer.writerow([f"Query{j}_Mongo",]) #diamo i nomi di intestazioni delle colonne
                writer.writerows([[tempo] for tempo in tempi])
            client.close()#chiusura connessione

    print("Avvio della fase di Genereazione grafici neo4j e MongoDB")
    #subprocess.run(["python", "grafici.py"]) eseguiamo un sottoprocesso per l'esecuzione del file che genera i grafici
    #subprocess.run(["python", "grafico_confronto.py"])


if __name__ == "__main__":
    numero_aziende_totali=300
    numero_aziende_italiane=250
    numero_transazioni=100
    numero_prodotti=30
    main(numero_aziende_totali,numero_aziende_italiane,numero_transazioni,numero_prodotti)
    input("Premi per l'esecuzione usando il 75 per cento dei dati")
    #75%
    main(
        int(numero_aziende_totali * 0.75),
        int(numero_aziende_italiane * 0.75),
        int(numero_transazioni * 0.75),
        int(numero_prodotti * 0.75)
    )
    input("Premi per l'esecuzione usando il 50 per cento dei dati")
    #50%
    main(
        int(numero_aziende_totali * 0.50),
        int(numero_aziende_italiane * 0.50),
        int(numero_transazioni * 0.50),
        int(numero_prodotti * 0.50)
    )
    input("Premi per l'esecuzione usando il 25 per cento dei dati")
    #25%
    main(
        int(numero_aziende_totali * 0.25),
        int(numero_aziende_italiane * 0.25),
        int(numero_transazioni * 0.25),
        int(numero_prodotti * 0.25)
    )

    

