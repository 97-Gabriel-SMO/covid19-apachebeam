""" Pipeline de dados ApacheBeam gerador de estatisticas de covid19

Autor: Gabriel Santos Madruga Oliveira
"""
import logging
import click
import apache_beam as beam
from utils import *


LOGGER = logging.getLogger(__name__)


@click.command()
@click.option("--datafile", "-d", type=str, help="File with covid-19 statistics.")
@click.option("--labelfile", "-l", type=str, help="File with states data.")
def main(datafile: str, labelfile: str):
    LOGGER.info("Starting process...")

    csv_rows = preprocessing_data("data/" + datafile, delimiter=";")
    label_rows = preprocessing_lable("data/" + labelfile, delimiter=";")
    with beam.Pipeline() as pipeline:
        processed_data = (
            pipeline
            | beam.Create(csv_rows)
            | beam.GroupBy("Codigo", "Regiao", "UF")
            .aggregate_field("casosNovos", sum, "totalCasos")
            .aggregate_field("obitosNovos", sum, "totalObitos")
            | beam.Map(join_data, label_rows)
            | beam.Filter(remove_missing_values)
        )
        LOGGER.debug("Generating csv output file...")
        data_export_csv = (
            processed_data
            | "Format CSV string" >> beam.Map(format_output_csv)
            | "Write to CSV output file"
            >> beam.io.WriteToText(
                "output/test_response",
                header=("Regiao;Estado;UF;Governador;TotalCasos;TotalObitos"),
                file_name_suffix=".csv",
                shard_name_template="",
            )
        )
        LOGGER.debug("DONE")
        LOGGER.debug("Generating json output file...")
        data_export_json = (
            processed_data
            | "Format JSON string" >> beam.Map(format_output_json)
            | beam.combiners.ToList()
            | "Write to JSON output file"
            >> beam.io.WriteToText(
                "output/test_response",
                file_name_suffix=".json",
                shard_name_template="",
            )
        )
        LOGGER.debug("DONE")
        LOGGER.debug("Files in output directory.")


if __name__ == "__main__":
    logging.basicConfig(filename="logs/log.cur")
    LOGGER.setLevel(logging.INFO)
    main()
