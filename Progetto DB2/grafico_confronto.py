import pandas as pd

from openpyxl import Workbook


def excel(lista):
    # Creo un file Excel vuoto
    wb = Workbook()
    wb.save('output_accodato.xlsx')

    # Ora ci scrivo sopra
    with pd.ExcelWriter('output_accodato.xlsx', engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        start_row = 0
        for lista in dati_liste:
            df = pd.DataFrame(lista, columns=['Lettera', 'Numero'])
            df.to_excel(writer, sheet_name='Dati', index=False, startrow=start_row)

            # Calcolo la prossima riga dove scrivere
            start_row += len(df) + 1  # +1 per lasciare una riga vuota se vuoi

