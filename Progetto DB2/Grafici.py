import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob

sns.set_theme(style="whitegrid")

# Prendi tutti i file CSV dei tempi generati
file_csv = glob.glob("tempi_*.csv") 

for file in file_csv:
    df = pd.read_csv(file)

   
    df_run = df[df["Nome_query"] != "Media"]
    df_media = df[df["Nome_query"] == "Media"]

    plt.figure(figsize=(12, 6))
    sns.barplot(data=df_run, x="run_id", y="tempo_ms", color="skyblue")

    if not df_media.empty:
        media_valore = df_media["tempo_ms"].values[0]
        plt.axhline(media_valore, color="red", linestyle="--", label=f"Media: {media_valore:.2f} ms")

    titolo = file.replace(".csv", "")
    plt.title(f"Tempi esecuzione: {titolo}", fontsize=16)
    plt.xlabel("Run ID", fontsize=12)
    plt.ylabel("Tempo (ms)", fontsize=12)

    
    if not df_media.empty:
        plt.legend()

   
    png_file = f"{titolo}_grafico.png"
    plt.savefig(png_file)
    plt.close()  
    print(f"Salvato grafico: {png_file}")


