from faker import Faker
from Database import Mongo_Connect,driver_neo4j,chiusura_Driver_neo4j
from random import random,randint,choice,uniform
import datetime


faker=Faker()

#elimina tutti i nodi dal db
def elimina_tutto(tx):
    tx.run("MATCH (n) DETACH DELETE n")

#i dataset ci servono anche per inserire i dati in MongoDB
def converti_date(dataset):
    for record in dataset:
        d = record.get("data")
        if isinstance(d, datetime.date) and not isinstance(d, datetime.datetime):
            record["data"] = datetime.datetime(d.year, d.month, d.day)


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


def prima_transazione_estero(tx,id_azienda,id_prodotto,numero_aziende_totali,numero_aziende_italiane,datasetTransazioni):
    importo=float(faker.pydecimal(left_digits=5, right_digits=2, positive=True))
    id_venditore=randint(numero_aziende_italiane,numero_aziende_totali-1) #scelgo casualemente un'azienda estera dalla quale fare la prima transazione
    transazione={
        "id_transazione":len(datasetTransazioni)+1,
        "data":faker.date_between(start_date="-7y", end_date="today"),
        "importo_totale":importo,
        "valuta":"€",
        "IVA_addebitata": 0,
        "IVA_recuperata": 0,
        "id_venditore": id_venditore,
        "id_acquirente": id_azienda,
        "id_prodotto": id_prodotto
    }
    tx.run(
        """
            MATCH(italiana:Azienda{id_azienda: $id_azienda})
            MATCH(p:Prodotto{id_prodotto: $id_prodotto})
            MATCH(estera:Azienda{id_azienda:$id_azienda_estera})
            CREATE (t:Transazione {
            id_transazione: $id,
            data: $data,
            importo_totale: $importo,
            valuta: $valuta,
            IVA_addebitata: $iva_addeb,
            IVA_recuperata: $iva_rec
            })
            CREATE (italiana)-[:EFFETTUA]->(t)-[:CONTIENE]->(p)-[:VENDUTO]->(estera)
        """,
            id_azienda=id_azienda,
            id_prodotto=id_prodotto,
            id_azienda_estera=id_venditore,#prende l id casuale di un'azienda estera
            id=transazione["id_transazione"],
            data=transazione["data"],
            importo=transazione["importo_totale"],
            valuta=transazione["valuta"],
            iva_addeb=transazione["IVA_addebitata"],
            iva_rec=transazione["IVA_recuperata"]
    ) 
    datasetTransazioni.append(transazione)



#usiamo tutte le aziende che abbiamo nella lista e colleghiamole con le transazioni
def crea_transazione_nazionale(tx, id_prodotto, id_azienda_venditrice, id_azienda_acquirente, datasetTransazioni):
    #cerchiamo se l'azienda ha già acquistato quel prodotto
    risultato = tx.run(
        """
        MATCH (a:Azienda {id_azienda: $id_azienda_venditrice})-[:EFFETTUA]->(t:Transazione)-[:CONTIENE]->(p:Prodotto {id_prodotto: $id_prodotto})
        RETURN t.importo_totale AS importo
        """,
        id_azienda_venditrice=id_azienda_venditrice,
        id_prodotto=id_prodotto
    )
    importi = [record["importo"] for record in risultato]
    importo_precedente = importi[0] if importi else None

    nuovo_id = len(datasetTransazioni) + 1
    #genero la data della transazione
    data_tx = faker.date_between(start_date="-7y", end_date="today")

    if importo_precedente is None:
        #creiamo un prezzo casuale per la prima vendita
        importo = randint(1, 1500)
    else:
        #rivendiamo al 10% in più
        importo = round(importo_precedente * 1.10, 2)

    iva_addeb = round(importo * 0.22, 2)
    iva_rec   = round(importo * uniform(0, 0.22), 2)

    tx.run(
        """
        MATCH (venditrice:Azienda {id_azienda: $id_azienda_venditrice})
        MATCH (acquirente:Azienda {id_azienda: $id_azienda_acquirente})
        MATCH (p:Prodotto {id_prodotto: $id_prodotto}) 
        CREATE (t:Transazione {
            id_transazione: $id_transazione,
            data: $data,
            importo_totale: $importo,
            valuta: '€',
            IVA_addebitata: $iva_addeb,
            IVA_recuperata: $iva_rec
        })
        CREATE (venditrice)-[:EFFETTUA]->(t)-[:CONTIENE]->(p)-[:VENDUTO_A]->(acquirente)
        """,
        id_azienda_venditrice=id_azienda_venditrice,
        id_azienda_acquirente=id_azienda_acquirente,
        id_prodotto=id_prodotto,
        id_transazione=nuovo_id,
        data=data_tx,
        importo=importo,
        iva_addeb=iva_addeb,
        iva_rec=iva_rec
    )

    #aggiugniamo al dataset
    transazione = {
        "id_transazione": nuovo_id,
        "data": data_tx,
        "importo_totale": importo,
        "valuta": "€",
        "IVA_addebitata": iva_addeb,
        "IVA_recuperata": iva_rec,
        "id_venditore": id_azienda_venditrice,
        "id_acquirente": id_azienda_acquirente,
        "id_prodotto": id_prodotto
    }
    datasetTransazioni.append(transazione)



    
    

def crea_catena (tx,id_aziende,id_prodotto,numero_aziende_totali,numero_aziende_italiane,datasetTransazioni):
    casuale=randint(1,10)  #genera un numero casuale da 1 a 10   
    if(casuale<3):
        #creo la prima transazione in cui l'azienda italiana acquista da quella estera non pagando l'iva
        prima_transazione_estero(tx,id_aziende[0],id_prodotto,numero_aziende_totali,numero_aziende_italiane,datasetTransazioni)
        #imposto il valore di casuale maggiore di 3, in modo tale da non rieseguire questa condizione nei cicli successivi
    for j in range(0,len(id_aziende)-1):
            crea_transazione_nazionale(tx,id_prodotto, id_aziende[j], id_aziende[j+1], datasetTransazioni)          
    #crea una transazione tra due aziende italiane 
            
            


def popola_Neo4j_MongoDB(numero_aziende_totali,numero_aziende_italiane,numero_transazioni,numero_prodotti):


    print("siamo entrati in popola_DB\n")
    neo4j_driver=driver_neo4j()


    paesi_esterni = ["Francia", "Germania", "Spagna", "Paesi Bassi", "Belgio", "Austria"]
    #creo le liste per mettere i record
    datasetAzienda=[] 
    datasetProdotti=[]
    datasetTransazioni=[]
    #aggiungo i record dentro il dataset
    print("Generazione in corso delle aziende e dei prodotti.")
    for i in range(1,numero_aziende_totali):
        #le aziende italiane avranno id che va da 0 a 2999
        if(i<numero_aziende_italiane):
            azienda={
                "id_azienda":i,
                "nomeAzienda":faker.company(),
                "Paese":"Italia",
                "data_Fondazione": faker.date()
            }
        else:
            #le aziende estere avranno un id che va da 3000 a 3499
            azienda={
            "id_azienda":i,
            "nomeAzienda":faker.company(),
            "Paese":choice(paesi_esterni),
            "data_Fondazione": faker.date()
        }  
        datasetAzienda.append(azienda)

    #generazione prodotti
    for k in range(1,numero_prodotti):
        prodotto={
            "id_prodotto":k,
            "tipo":faker.word()
        }
        datasetProdotti.append(prodotto)

    #Abbiamo riempito il dataset
    #bisogna ora inserirlo in neo4j e in mongodb

    #immettiamo i nodi nel db
    print("stiamo per immettere i nodi azienda e prodotti nel database neo4j\n")
    with neo4j_driver.session() as session:
        # cancella tutto prima di inserire i dati
        session.execute_write(elimina_tutto)
        print("tutti i nodi sono stati eliminati dal db neo4j")
        for record in datasetAzienda:
                #immettiamo i nodi azienda nel db
                session.execute_write(crea_azienda, record)
        for record in datasetProdotti:
                #immettiamo i nodi prodotto nel db
                session.execute_write(crea_prodotto, record)

    #creiamo le relazioni dentro neo4j
    lista_id_aziende=[]
    print("stiamo per generare la sessione di connessione al database neo4j per la creazione delle transazioni\n")
    with neo4j_driver.session() as session:
        #decidiamo quante transazioni vogliamo creare

        #ATTENZIONE, il numero di transazioni che mettiamo in realtà rappresenta il numero di catene_di_transazioni 
        #che vogliamo generare. Ogni singola catena avrà dalle 3 alle 6 transazioni

        for i in range(1, numero_transazioni):
            #decidiamo quanto deve essere grande la catena di transazione
            numero_random=randint(3,7)
            for j in range (1,numero_random):
                #immagazziniamo un numero random di id aziende dentro una lista per poi creare una catena di transazioni per un prodotto
                #esempio: numero random=4, allora lista_id_aziende avrà elementi dalla cella 0 alla cella 3 e faremo 3 relaizoni
                id_azienda_random = faker.random_int(min=1, max=numero_aziende_totali-1)
                lista_id_aziende.append(id_azienda_random)
            id_prodotto_random = faker.random_int(min=1, max=numero_prodotti-1) #pesca l'id di un prodotto casualemente da quelli generati
            session.execute_write(crea_catena,lista_id_aziende,id_prodotto_random,numero_aziende_totali,numero_aziende_italiane,datasetTransazioni)
            #aggiungo a queste liste le relazioni create, mi serviranno dopo per creare le stesse relazioni in mongoDB
            lista_id_aziende=[] #svuota la lista dopo averla usata

#Parte di MongoDB
    client,db=Mongo_Connect()

    #se non esistono crea le collezioni e le assegna a queste variabili
    aziende_collection = db["aziende"]
    transazioni_collection = db["transazioni"]
    prodotti_collection = db["prodotti"]

    #elimino i dati dentro le collezioni se presenti
    print("Prima del drop:", aziende_collection.count_documents({}))
    aziende_collection.drop()
    print("Dopo il drop:", aziende_collection.count_documents({})) 
    prodotti_collection.drop()
    transazioni_collection.drop()

    #inseriamo i dati nel db mongo
    aziende_collection.insert_many(datasetAzienda)
    prodotti_collection.insert_many(datasetProdotti)
    converti_date(datasetTransazioni)  #convertiamo le date in un formato leggibile da mongo
    transazioni_collection.insert_many(datasetTransazioni)

    print("i dati sono stati inseriti , chiusura connessioni in corso\n")
    #chiudo le connessioni
    client.close()
    neo4j_driver.close()
    

#bisogna gestire le relazioni ora
#nella collezione transazioni, aggiungi  id_azienda che collega la transazione all’azienda.
# nella collezione transazioni, puoi aggiungere un campo prodotti (lista di id_prodotto)


