""" Pipeline de dados ApacheBeam gerador de estatisticas de covid19

Autor: Gabriel Santos Madruga Oliveira
"""
import apache_beam as beam
from utils import preprocessing_data_csv,preprocessing_lable_csv,join_data,remove_missing_values


def main():
    csv_rows = preprocessing_data_csv('data/HIST_PAINEL_COVIDBR_28set2020.csv',';')
    label_rows = preprocessing_lable_csv('data/EstadosIBGE.csv',';')
    with beam.Pipeline() as pipeline:
        processed_data = (
            pipeline
            | beam.Create(csv_rows)
            | beam.GroupBy('Codigo','Regiao','UF')
                  .aggregate_field('casosNovos', sum, 'totalCasos')
                  .aggregate_field('obitosNovos', sum, 'totalObitos')
            | beam.Map(join_data,label_rows)
            | beam.Filter(remove_missing_values)
            )


if __name__ == '__main__':
    main()