from pymongo import MongoClient, errors
from neo4j import GraphDatabase, exceptions
import sys

MONGO_URI = "mongodb://localhost:27017"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "alberto135"
DB_NAME = "test"

def Mongo_Connect():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)  #
        # prova a forzare la connessione (lancia un'eccezione se fallisce)
        client.admin.command('ping')
        db = client[DB_NAME]  # seleziona il database 'test'
        return client, db
    except errors.ServerSelectionTimeoutError as e:
        print("Errore: impossibile connettersi a MongoDB.")
        print("Dettagli:", e)
        sys.exit(1)

def driver_neo4j():
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        # test connessione aprendo una sessione
        with driver.session() as session:
            session.run("RETURN 1")
        return driver
    except exceptions.ServiceUnavailable as e:
        print("Errore: impossibile connettersi al database Neo4j.")
        print("Dettagli:", e)
        sys.exit(1)
    except Exception as e:
        print("Errore generico nella connessione Neo4j:")
        print(e)
        sys.exit(1)

def chiusura_Driver_neo4j(driver):
    driver.close()
