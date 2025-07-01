import time
from pymongo import MongoClient
from neo4j import GraphDatabase

# Configuration
MONGO_URI = "mongodb://localhost:27017"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "alberto135"
DB_NAME = "test"

# Initialize clients
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client["test"]

neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

# Sample data loader
def load_data():
    # Clear existing data
    mongo_db.drop_collection("companies")
    mongo_db.drop_collection("persons")
    mongo_db.drop_collection("holdings")
    mongo_db.drop_collection("sells")
    mongo_db.drop_collection("directors")
    mongo_db.drop_collection("controls")

    with neo4j_driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    
    # Insert into MongoDB
    companies = [
        {"name": "Southern Europa Telco", "country": "Italy", "creation_epoch": 1609459200},
        {"name": "Joint Bridge Co.", "country": "UK", "creation_epoch": 1609545600},
        {"name": "Joint IT Group", "country": "Italy", "creation_epoch": 1609632000},
        {"name": "Swift Co.", "country": "USA", "creation_epoch": 1609718400},
    ]
    cids = mongo_db.companies.insert_many(companies).inserted_ids

    persons = [{"name": "Cletis Bysshe"}]
    pids = mongo_db.persons.insert_many(persons).inserted_ids

    holdings = [{"name": "Joint Holdings Ltd"}]
    hids = mongo_db.holdings.insert_many(holdings).inserted_ids

    sells = [
        {"from_company_id": cids[0], "to_company_id": cids[1], "amount": 50000, "epoch": 1609462800},
        {"from_company_id": cids[1], "to_company_id": cids[2], "amount": 60000, "epoch": 1609466400},
        {"from_company_id": cids[2], "to_company_id": cids[3], "amount": 70000, "epoch": 1609470000},
    ]
    mongo_db.sells.insert_many(sells)

    mongo_db.directors.insert_one({"person_id": pids[0], "company_id": cids[0]})
    mongo_db.controls.insert_many([{"holding_id": hids[0], "company_id": cids[1]}, {"holding_id": hids[0], "company_id": cids[2]}])

    # Insert into Neo4j
    with neo4j_driver.session(database="neo4j") as session:
        session.run(
            """
            CREATE (c1:Company {name: $n1, country: $c1, creation_epoch: $e1}),
                   (c2:Company {name: $n2, country: $c2, creation_epoch: $e2}),
                   (c3:Company {name: $n3, country: $c3, creation_epoch: $e3}),
                   (c4:Company {name: $n4, country: $c4, creation_epoch: $e4})
            CREATE (p1:Person {name: $pname})
            CREATE (h1:Holding {name: $hname})
            CREATE (c1)-[:SELLS_TO {amount: 50000, epoch: 1609462800}]->(c2),
                   (c2)-[:SELLS_TO {amount: 60000, epoch: 1609466400}]->(c3),
                   (c3)-[:SELLS_TO {amount: 70000, epoch: 1609470000}]->(c4),
                   (p1)-[:DIRECTOR_OF]->(c1),
                   (h1)-[:CONTROLS]->(c2),
                   (h1)-[:CONTROLS]->(c3)
            """,
            {
                "n1": companies[0]["name"], "c1": companies[0]["country"], "e1": companies[0]["creation_epoch"],
                "n2": companies[1]["name"], "c2": companies[1]["country"], "e2": companies[1]["creation_epoch"],
                "n3": companies[2]["name"], "c3": companies[2]["country"], "e3": companies[2]["creation_epoch"],
                "n4": companies[3]["name"], "c4": companies[3]["country"], "e4": companies[3]["creation_epoch"],
                "pname": persons[0]["name"], "hname": holdings[0]["name"]
            }
        )

# Define Queries
queries = [
    {
        "name": "All Companies",
        "mongo": lambda: list(mongo_db.companies.find({})),
        "neo4j": lambda session: session.run("MATCH (c:Company) RETURN c.name AS name").data()
    },
    {
        "name": "Companies in Italy",
        "mongo": lambda: list(mongo_db.companies.find({"country": "Italy"})),
        "neo4j": lambda session: session.run("MATCH (c:Company {country: 'Italy'}) RETURN c.name AS name").data()
    },
    {
        "name": "Sales Relationships",
        "mongo": lambda: list(mongo_db.sells.aggregate([
            {"$lookup": {"from": "companies", "localField": "from_company_id", "foreignField": "_id", "as": "from_comp"}},
            {"$unwind": "$from_comp"},
            {"$lookup": {"from": "companies", "localField": "to_company_id", "foreignField": "_id", "as": "to_comp"}},
            {"$unwind": "$to_comp"},
            {"$project": {"from": "$from_comp.name", "to": "$to_comp.name", "amount": 1}}
        ])),
        "neo4j": lambda session: session.run(
            "MATCH (a)-[r:SELLS_TO]->(b) RETURN a.name AS from, b.name AS to, r.amount AS amount"
        ).data()
    },
    {
        "name": "Person Director Of",
        "mongo": lambda: list(mongo_db.directors.aggregate([
            {"$lookup": {"from": "persons", "localField": "person_id", "foreignField": "_id", "as": "p"}},
            {"$unwind": "$p"},
            {"$lookup": {"from": "companies", "localField": "company_id", "foreignField": "_id", "as": "c"}},
            {"$unwind": "$c"},
            {"$project": {"person": "$p.name", "company": "$c.name"}}
        ])),
        "neo4j": lambda session: session.run(
            "MATCH (p:Person)-[:DIRECTOR_OF]->(c:Company) RETURN p.name AS person, c.name AS company"
        ).data()
    },
    {
        "name": "Holding Controls",
        "mongo": lambda: list(mongo_db.controls.aggregate([
            {"$lookup": {"from": "holdings", "localField": "holding_id", "foreignField": "_id", "as": "h"}},
            {"$unwind": "$h"},
            {"$lookup": {"from": "companies", "localField": "company_id", "foreignField": "_id", "as": "c"}},
            {"$unwind": "$c"},
            {"$project": {"holding": "$h.name", "company": "$c.name"}}
        ])),
        "neo4j": lambda session: session.run(
            "MATCH (h:Holding)-[:CONTROLS]->(c:Company) RETURN h.name AS holding, c.name AS company"
        ).data()
    }
]

# Measure performance
def time_query(fn):
    start = time.time()
    _ = fn()
    return time.time() - start


def run_tests():
    results = []
    load_data()
    with neo4j_driver.session() as session:
        for q in queries:
            t_mongo = time_query(q["mongo"])
            t_neo4j = time_query(lambda: q["neo4j"](session))
            results.append({
                "query": q["name"],
                "mongo_time": t_mongo,
                "neo4j_time": t_neo4j
            })
    return results

if __name__ == "__main__":
    res = run_tests()
    for r in res:
        print(f"{r['query']}: MongoDB={r['mongo_time']:.6f}s | Neo4j={r['neo4j_time']:.6f}s") 