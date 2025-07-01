from pymongo import MongoClient
from neo4j import GraphDatabase

MONGO_URI = "mongodb://localhost:27017"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "alberto135"
DB_NAME = "test"

#Attraverso il client puoi gestire la connessione, fare query, chiudere la connessione
#db è una cartella virtuale dentro MongoDB dove sono organizzati i dati.
def Mongo_Connect():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]  # seleziona il database 'test'
    return client, db

def driver_neo4j():
     driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
     return driver


def chiusura_Driver_neo4j(driver):
    driver.close()
