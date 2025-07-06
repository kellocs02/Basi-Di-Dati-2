import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob

sns.set_theme(style="whitegrid")

file_neo4j = sorted(glob.glob("tempi_Query_Neo4j_*.csv"))
file_mongo = sorted(glob.glob("tempi_Query_Mongo*.csv"))

numero_query = min(len(file_neo4j), len(file_mongo))

for i in range(numero_query):
    df_neo4j = pd.read_csv(file_neo4j[i])
    df_mongo = pd.read_csv(file_mongo[i])

    media_neo4j = df_neo4j[df_neo4j["run_id"] == "-"]["tempo_ms"].values
    media_mongo = df_mongo[df_mongo["run_id"] == "-"]["tempo_ms"].values

    if len(media_neo4j) == 0 or len(media_mongo) == 0:
        print(f"Attenzione: manca la riga media per la query {i}, salto il grafico.")
        continue

    
    df_medie = pd.DataFrame({
        "Database": ["Neo4j", "MongoDB"],
        "Tempo medio (ms)": [media_neo4j[0], media_mongo[0]]
    })

    plt.figure(figsize=(8, 6))
    sns.barplot(data=df_medie, x="Database", y="Tempo medio (ms)", hue="Database", dodge=False, palette=["#1f77b4", "#ff7f0e"])

    plt.title(f"Confronto medie tempi esecuzione Query {i}", fontsize=16)
    plt.ylabel("Tempo medio (ms)", fontsize=14)
    plt.xlabel("Database", fontsize=14)
    leg = plt.gca().get_legend()
    if leg:
         leg.remove()  
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()

    plt.savefig(f"Confronto_Medie_Query_{i}.png")
    print(f"Creato grafico medie: Confronto_Medie_Query_{i}.png")
    plt.close()

print("Tutti i grafici delle medie sono stati creati!")

