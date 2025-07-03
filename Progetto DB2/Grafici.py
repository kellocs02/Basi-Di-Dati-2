import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob

sns.set_theme(style="whitegrid")

# Prendi tutti i file CSV dei tempi generati
file_csv = glob.glob("tempi_*.csv")  # es. tempi_0.csv, tempi_1.csv, ...

for file in file_csv:
    df = pd.read_csv(file)

    plt.figure(figsize=(12, 6))
    sns.barplot(data=df, x="run_id", y="tempo_ms", color="skyblue")

    titolo = file.replace(".csv", "")
    plt.title(f"Tempi esecuzione: {titolo}", fontsize=16)
    plt.xlabel("Run ID", fontsize=12)
    plt.ylabel("Tempo (ms)", fontsize=12)

    # Salva il grafico come immagine
    png_file = f"{titolo}_grafico.png"
    plt.savefig(png_file)
    print(f"Salvato grafico: {png_file}")

