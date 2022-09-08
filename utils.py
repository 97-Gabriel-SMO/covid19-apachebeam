""" Arquivo com funções auxiliares utilizadas no codigo.

Autor: Gabriel Santos Madruga Oliveira
"""

import csv
import apache_beam as beam



def preprocessing_data_csv(filepath:str,delimiter:str):
    with open(filepath, mode = 'r', encoding='utf-8-sig') as csvfile:
        collection_result = []
        reader = csv.DictReader(csvfile,delimiter=delimiter)
        for row in reader:                
            collection_result.append(beam.Row(Regiao=row['regiao'],
                                            UF=row['estado'],
                                            casosNovos=int(row['casosNovos']),
                                            obitosNovos=int(row['obitosNovos']),
                                            Codigo=row['coduf']))
        return collection_result
                    

def preprocessing_lable_csv(filepath:str,delimiter:str):
    with open(filepath, mode = 'r', encoding='utf-8-sig') as csvfile:
        collection_result = []
        reader = csv.DictReader(csvfile,delimiter=delimiter)
        for row in reader:
            collection_result.append(beam.Row(Governador=row['Governador [2019]'],
                                            Estado=row['UF [-]'],
                                            Codigo=row['Código [-]']))
        return collection_result



def remove_missing_values(row):
    for value in row:
        if value == "":
            return False
    return True


