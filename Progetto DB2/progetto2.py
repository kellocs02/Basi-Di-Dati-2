import time
import InserimentoDati
import csv
from pymongo import MongoClient
from neo4j import GraphDatabase
from Database import Mongo_Connect, driver_neo4j, chiusura_Driver_neo4j
import subprocess
import os
import glob

def pulisci_cartella():
    # Cancella tutti i CSV
    for file_csv in glob.glob("*.csv"):
        try:
            os.remove(file_csv)
            print(f"Rimosso file CSV: {file_csv}")
        except Exception as e:
            print(f"Errore rimuovendo {file_csv}: {e}")
    
    # Cancella tutti i PNG
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


def main():
    pulisci_cartella()
    numero_aziende_totali=100
    numero_aziende_italiane=80
    numero_transazioni=160
    numero_prodotti=20
    print("Avvio software Progetto Database NoSQL")
    input("Premi Invio quando sei pronto per popolare il database...")
    InserimentoDati.popola_Neo4j_MongoDB(numero_aziende_totali,numero_aziende_italiane,numero_transazioni,numero_prodotti)
    print("Database riempiti\nFase di esecuzione query\n")

    #preparazione driver e CSV
    #lista delle query
    querys = ["""
        MATCH (a:Azienda)
        RETURN a.id_azienda AS Azienda_ID
        """,
        """
        MATCH (a:Azienda)-[:EFFETTUA]->(t:Transazione)
        RETURN a.nome AS nome_azienda, t.id_transazione AS id_transazione
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
        RETURN 
            a.id_azienda AS id_azienda, 
            p.id_prodotto AS id_prodotto, 
            t.importo_totale AS importo
        ORDER BY t.importo_totale DESC
        """,
        """
        MATCH (a:Azienda)
        OPTIONAL MATCH (a)-[:EFFETTUA]->(t:Transazione)-[:CONTIENE]->(p:Prodotto)
        WITH 
            a.id_azienda AS id_azienda,
            COUNT(DISTINCT t) AS num_transazioni,
            SUM(
                CASE 
                    WHEN t IS NOT NULL AND t.importo_totale IS NOT NULL THEN t.importo_totale 
                    ELSE 0 
                END
            ) AS totale_importi
        RETURN 
            id_azienda,
            num_transazioni,
            totale_importi
        ORDER BY totale_importi DESC

        """   
        ]
    for i,query in enumerate(querys):
        nome_file = f"tempi_Query_Neo4j_{i}.csv"  #modifico il nome del file csv ad ogni iterazione
        with open(nome_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile) #apriamo il file csv in sola scrittura
            writer.writerow(["Nome_query", "run_id", "tempo_ms"]) #diamo i nomi di intestazioni delle colonne

            #prima esecuzione vergine (run_id = 0)
            neo4j_driver = driver_neo4j()   #apro connessione
            elapsed, records = esegui_query_neo4j(neo4j_driver, query) #esegue la query
            neo4j_driver.close()            #chiudo connessione
            writer.writerow(["Azienda_ID_query", 0, elapsed]) #mette i nomi delle intestazioni
            print(f"Run 0: {elapsed:.2f} ms, {len(records)} record trovati") #stampa il tempo in ms con due cifre decimali e dice il numero di record trovati
            for record in records:
                for key, value in record.items(): #stampiamo i vari campi dei record trovati
                    print(f"{key}: {value}")
                print("---")
            #esecuzioni da 1 a 30
            neo4j_driver = driver_neo4j() 
            for run_id in range(1, 31):
                elapsed, records = esegui_query_neo4j(neo4j_driver, query)
                writer.writerow(["Azienda_ID_query", run_id, elapsed])
                print(f"Run {run_id}: {elapsed:.2f} ms, {len(records)} record trovati")
            neo4j_driver.close()   


    #parte di mongoDB
    #creiamo le query per mongoDB
    pipelines = [
    # 1) MATCH (a:Azienda) RETURN a.id_azienda AS Azienda_ID
            [
                { "$project": { "_id": 0, "id_azienda": 1 } }
            ],

            # 2) MATCH (a:Azienda)-[:EFFETTUA]->(t:Transazione)
            #    RETURN a.nome AS nome_azienda, t.id_transazione AS id_transazione
            [
                { "$lookup": {
                    "from":       "Transazione",
                    "localField": "id_azienda",
                    "foreignField":"id_azienda",
                    "as":         "transazioni"
                }},
                { "$unwind": "$transazioni" },
                { "$project": {
                    "_id":            0,
                    "nome_azienda":   "$nome",
                    "id_transazione":"$transazioni.id_transazione"
                }}
            ],

            # 3) MATCH (a:Azienda)-[:EFFETTUA]->(t:Transazione)-[:CONTIENE]->(p:Prodotto)
            #    RETURN a.id_azienda, p.id_prodotto, t.importo_totale AS importo
            #    ORDER BY t.importo_totale DESC
            [
                { "$lookup": {
                    "from":        "Transazione",
                    "localField":  "id_azienda",
                    "foreignField":"id_azienda",
                    "as":          "transazioni"
                }},
                { "$unwind": "$transazioni" },
                { "$lookup": {
                    "from":        "Prodotto",
                    "localField":  "transazioni.id_transazione",
                    "foreignField":"id_transazione",
                    "as":          "prodotti"
                }},
                { "$unwind": "$prodotti" },
                { "$project": {
                    "_id":            0,
                    "id_azienda":     "$id_azienda",
                    "id_prodotto":    "$prodotti.id_prodotto",
                    "importo":        "$transazioni.importo_totale"
                }},
                { "$sort": { "importo": -1 } }
            ],

            # 4) stessa di prima + WHERE t.importo_totale > 500
            [
                { "$lookup": {
                    "from":        "Transazione",
                    "localField":  "id_azienda",
                    "foreignField":"id_azienda",
                    "as":          "transazioni"
                }},
                { "$unwind": "$transazioni" },
                { "$match": { "transazioni.importo_totale": { "$gt": 500 } } },
                { "$lookup": {
                    "from":        "Prodotto",
                    "localField":  "transazioni.id_transazione",
                    "foreignField":"id_transazione",
                    "as":          "prodotti"
                }},
                { "$unwind": "$prodotti" },
                { "$project": {
                    "_id":            0,
                    "id_azienda":     "$id_azienda",
                    "id_prodotto":    "$prodotti.id_prodotto",
                    "importo":        "$transazioni.importo_totale"
                }},
                { "$sort": { "importo": -1 } }
            ],

            # 5) MATCH Azienda + OPTIONAL MATCH Transazioni/Prodotti +
            #    COUNT DISTINCT transazioni, SUM importi (null→0), ORDER BY
            [
                { "$lookup": {
                    "from":        "Transazione",
                    "localField":  "id_azienda",
                    "foreignField":"id_azienda",
                    "as":          "transazioni"
                }},
                # preserva anche aziende senza transazioni
                { "$unwind": {
                    "path":                      "$transazioni",
                    "preserveNullAndEmptyArrays": True
                }},
                { "$group": {
                    "_id":             "$id_azienda",
                    "num_transazioni": { "$sum": {
                        "$cond": [ { "$ifNull": [ "$transazioni", False ] }, 1, 0 ]
                    }},
                    "totale_importi":  { "$sum": {
                        "$ifNull": [ "$transazioni.importo_totale", 0 ]
                    }}
                }},
                { "$project": {
                    "_id":            0,
                    "id_azienda":     "$_id",
                    "num_transazioni":"$num_transazioni",
                    "totale_importi": "$totale_importi"
                }},
                { "$sort": { "totale_importi": -1 } }
            ]
        ]
    
    for j, pipeline in enumerate(pipelines):
        nome_file = f"tempi_Query_Mongo{j}.csv"
        with open(nome_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Nome_query", "run_id", "tempo_ms"])
            
            client, db = Mongo_Connect()  # apro connessione
            collection_name = "aziende"   # se vuoi cambiare, fai un dizionario con pipeline -> collezione
            
            #prima esecuzione vergine
            elapsed, records = esegui_query_mongodb(db, collection_name, pipeline)
            writer.writerow([f"MongoQuery{j}", 0, elapsed])
            print(f"Run 0: {elapsed:.2f} ms, {len(records)} record trovati")
            for record in records:
                for chiave, valore in record.items():#record.items restituisce la lista chiave-valore di record che è un dizionario
                    print(f"{chiave}: {valore}") #stampiamo chiave-valore
                print("---")
            
            for run_id in range(1, 31):
                elapsed, records = esegui_query_mongodb(db, collection_name, pipeline)
                writer.writerow([f"MongoQuery{j}", run_id, elapsed])
                print(f"Run {run_id}: {elapsed:.2f} ms, {len(records)} record trovati")
            
            client.close() #chiusura connessione

    print("Avvio della fase di Genereazione grafici neo4j e MongoDB")
    subprocess.run(["python", "grafici.py"])


if __name__ == "__main__":
    main()
    

