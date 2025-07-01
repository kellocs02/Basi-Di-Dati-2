from faker import Faker
import mysql.connector
from Database import Mongo_Connect,driver_neo4j,chiusura_Driver_neo4j
from random import random,randint


def crea_azienda(tx, azienda):
    #Quelle triple virgolette """ ... """ servono a definire una stringa multilinea in Python.
    tx.run(
        """
        CREATE (a:Azienda {
            id_azienda: $id,
            nomeAzienda: $nome,
            Paese: $paese,
            data_Fondazione: $data
        })
        """,
        id=azienda["id_azienda"],
        nome=azienda["nomeAzienda"],
        paese=azienda["Paese"],
        data=azienda["data_Fondazione"]
    )

def crea_transazione(tx, transazione):
    tx.run(
        """
        CREATE (t:Transazione {
            id_transazione: $id,
            data: $data,
            importo_totale: $importo,
            valuta: $valuta,
            IVA_addebitata: $iva_addeb,
            IVA_recuperata: $iva_rec
        })
        """,
        id=transazione["id_transazione"],
        data=transazione["data"],
        importo=transazione["importo_totale"],
        valuta=transazione["valuta"],
        iva_addeb=transazione["IVA_addebitata"],
        iva_rec=transazione["IVA_recuperata"]
    )

def crea_prodotto(tx, prodotto):
    tx.run(
        """
        CREATE (p:Prodotto {
            id_prodotto: $id,
            tipo: $tipo
        })
        """,
        id=prodotto["id_prodotto"],
        tipo=prodotto["tipo"]
    )

def crea_relazione_azienda_transazione(tx, id_azienda, id_transazione):
    tx.run(
        """
        MATCH (a:Azienda {id_azienda: $id_azienda})
        MATCH (t:Transazione {id_transazione: $id_transazione})
        CREATE (a)-[:EFFETTUA]->(t)
        """,
        id_azienda=id_azienda,
        id_transazione=id_transazione
    )
    

def crea_relazione_transazione_prodotto(tx, id_transazione, id_prodotto):
    tx.run(
        """
        MATCH (t:Transazione {id_transazione: $id_transazione})
        MATCH (p:Prodotto {id_prodotto: $id_prodotto})
        CREATE (t)-[:CONTIENTE]->(p)
        """,
        id_transazione=id_transazione,
        id_prodotto=id_prodotto
    )


def crea_catena (lunghezza_catena,id_aziende,id_prodotto):
    for i in lunghezza_catena:

     



    






#crea l'istanza faker in italiano
faker = Faker('it_IT')

neo4j_driver=driver_neo4j()

numero_aziende_estere=500
numero_aziende_italiane=3000
numero_transazioni=10000
numero_prodotti=1000
#creo una lista per mettere i record 
dataset=[]
datasetEstero=[]
#aggiungo i record dentro il dataset
for i in range(1,numero_aziende_italiane):
    azienda={
        "id_azienda":i,
        "nomeAzienda":faker.company(),
        "Paese":faker.country(),
        "data_Fondazione": faker.date()
    }
    dataset.append(azienda)

for h in range(1,numero_aziende_estere):
    azienda={
        "id_azienda":h,
        "nomeAzienda":faker.company(),
        "Paese":faker.country(),
        "data_Fondazione": faker.date()
    }
    dataset.append(azienda)

for j in range(1,numero_transazioni):
    importo=float(faker.pydecimal(left_digits=5, right_digits=2, positive=True))
    transazione={
        "id_transazione":j,
        "data":faker.date_between(start_date="-7y", end_date="today"),
        "importo_totale":importo,
        "valuta":faker.currency_symbol(),
        "IVA_addebitata": round(importo* 0.22, 2),
        "IVA_recuperata": round(importo * random.uniform(0, 0.22), 2) 
    }
    dataset.append(transazione)

for k in range(1,numero_prodotti):
    prodotto={
        "id_prodotto":k,
        "tipo":faker.word()
    }
    dataset.append(prodotto)

#Abbiamo riempito il dataset
#bisogna ora inserirlo in neo4j e in mongodb






#immettiamo i nodi nel db
with neo4j_driver.session() as session:
    for record in dataset:
        if "id_azienda" in record:
            # è un nodo Azienda
            session.execute_write(crea_azienda, record)
        elif "id_transazione" in record:
            # è un nodo Transazione
            session.execute_write(crea_transazione, record)
        elif "id_prodotto" in record:
            # è un nodo Prodotto
            session.execute_write(crea_prodotto, record)

#creiamo le relazioni debtri neo4j
lista_id_aziende=[]
lista_relazione_A_T=[]
lista_relazione_T_P=[]
prodotti_disponibili=[]
with neo4j_driver.session() as session:
    for i in range(1, numero_transazioni):
        numero_random=randint(3,7)
        for j in range (1,numero_random):
            #immagazziniamo un numero random di id aziende dentro una lista per poi creare una catena di transazioni per un prodotto
            id_azienda_random = faker.random_int(min=1, max=numero_aziende-1)
            lista_id_aziende.append(id_azienda_random)
        id_prodotto_random = faker.random_int(min=1, max=numero_prodotti-1)
        session.execute_write(crea_catena,len(lista_id_aziende)-1,lista_id_aziende,id_prodotto_random)
        session.execute_write(crea_relazione_azienda_transazione, id_azienda_random, i)
        session.execute_write(crea_relazione_transazione_prodotto, i, id_prodotto_random)
        #aggiungo a queste liste le relazioni create, mi serviranno dopo per creare le stesse relazioni in mongoDB
        lista_relazione_A_T.append((id_azienda_random,i))
        lista_relazione_T_P.append((id_prodotto_random,i))

#Parte di MongoDB
client,db=Mongo_Connect()

#se non esistono crea le collezioni e le assegna a queste variabili
aziende_collection = db["aziende"]
transazioni_collection = db["transazioni"]
prodotti_collection = db["prodotti"]

#diviamo il dataset in 3 parti
aziende = [r for r in dataset if "id_azienda" in r]
transazioni = [r for r in dataset if "id_transazione" in r]
prodotti = [r for r in dataset if "id_prodotto" in r]

#inseriamo i dati nel db mongo
aziende_collection.insert_many(aziende)
transazioni_collection.insert_many(transazioni)
prodotti_collection.insert_many(prodotti)

#bisogna gestire le relazioni ora
#nella collezione transazioni, aggiungi  id_azienda che collega la transazione all’azienda.
# nella collezione transazioni, puoi aggiungere un campo prodotti (lista di id_prodotto)


