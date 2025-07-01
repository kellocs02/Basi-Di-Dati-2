import time
from pymongo import MongoClient
from neo4j import GraphDatabase
from Database import Mongo_Connect, driver_neo4j, chiusura_Driver_neo4j

MONGO_URI = "mongodb://localhost:27017"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "alberto135"
DB_NAME = "test"
#creiamo il driver che ci serve per conenttersi al db
neo4j_driver = GraphDatabase.driver(NEO4J_URI,auth=(NEO4J_USER, NEO4J_PASS))
#ci connettiamo al database
with neo4j_driver.session(database="neo4j") as session: #Apre una sessione sul database chiamato fraud2
    result = session.run("MATCH (n) RETURN count(n) AS totNodi") #Esegue una query che mostra tutti i nodi e li restituisce con l'etichetta totNodi
                                                                 #restituisce un oggetto result che contiene vari record
    print(result.single()["totNodi"])  #stampaimo il valore del primo record di result ed estrare il valore associato al campo totNodi
    #Siamo connessione al db neo4j

    client = MongoClient("mongodb://localhost:27017") #connessione al server mongoDB
    db = client[DB_NAME] #Usiamo il database test

    #Creiamo le 3 collezioni che corrispondono alle tabelle
    aziende_col = db["aziende"]
    prodotti_col = db["prodotti"]
    transazioni_col = db["transazioni"]
    #possiamo inserire e modificare i dati nelle collezioni
    
# Dati Aziende
aziende = [
    {"Id_Azienda": 0, "nome": "Foreign Co",   "Paese": "Germania"},
    {"Id_Azienda": 1, "nome": "Alpha SRL",    "Paese": "Italia"},
    {"Id_Azienda": 2, "nome": "Beta SRL",     "Paese": "Italia"},
    {"Id_Azienda": 3, "nome": "Gamma SRL",    "Paese": "Italia"},
    {"Id_Azienda": 4, "nome": "Delta SRL",    "Paese": "Italia"},
    {"Id_Azienda": 5, "nome": "Epsilon SRL",  "Paese": "Italia"}
]

# Dati Prodotto unico
prodotti = [
    {"Id_Prodotto": 100, "nome": "Scheda Video RTX 4060", "tipo": "Informatica"}
]

# Dati Transazioni con riferimenti a venditore, acquirente e prodotto
transazioni = [
    {
        "Id_Transazione": 1,
        "data": "2025-02-01",
        "importo_netto": 1000,
        "IVA_addebitata": 0,
        "IVA_recuperata": 220,
        "venditore_id": 0,
        "acquirente_id": 1,
        "prodotto_id": 100,
        "quantita": 1,
        "prezzo_unitario": 1000
    },
    {
        "Id_Transazione": 2,
        "data": "2025-02-05",
        "importo_netto": 1200,
        "IVA_addebitata": 264,
        "IVA_recuperata": 264,
        "venditore_id": 1,
        "acquirente_id": 2,
        "prodotto_id": 100,
        "quantita": 1,
        "prezzo_unitario": 1200
    },
    {
        "Id_Transazione": 3,
        "data": "2025-02-10",
        "importo_netto": 1400,
        "IVA_addebitata": 308,
        "IVA_recuperata": 308,
        "venditore_id": 2,
        "acquirente_id": 3,
        "prodotto_id": 100,
        "quantita": 1,
        "prezzo_unitario": 1400
    },
    {
        "Id_Transazione": 4,
        "data": "2025-02-15",
        "importo_netto": 1600,
        "IVA_addebitata": 352,
        "IVA_recuperata": 352,
        "venditore_id": 3,
        "acquirente_id": 4,
        "prodotto_id": 100,
        "quantita": 1,
        "prezzo_unitario": 1600
    },
    {
        "Id_Transazione": 5,
        "data": "2025-02-20",
        "importo_netto": 1800,
        "IVA_addebitata": 396,
        "IVA_recuperata": 396,
        "venditore_id": 4,
        "acquirente_id": 5,
        "prodotto_id": 100,
        "quantita": 1,
        "prezzo_unitario": 1800
    }
]

#eliminamo i dati vecchi nel db
aziende_col.delete_many({})
prodotti_col.delete_many({})
transazioni_col.delete_many({})

#aggiugniamo quelli nuovi
aziende_col.insert_many(aziende)
prodotti_col.insert_many(prodotti)
transazioni_col.insert_many(transazioni)

#dobbiamo fare le query


print("Dati inseriti nel Db Mongo")
