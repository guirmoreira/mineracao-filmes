import datetime
import logging
from math import nan
import math
import re

import pandas as pd

from progress.bar import Bar

import data_extract_transform as det
import scrap_the_numbers as sb

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

exceptions = []
MAX_EXCEPTIONS = 100

df = pd.read_csv("movies_202505012047.csv", sep=';')

df_copy = df.copy()

def insert(db, column, index, value, prefix=None):
    if prefix:
        column = f"{prefix}_{column}"
    if column not in db.columns:
        db[column] = None
    db.at[index, column] = value


def save(db, file_path):
    try:
        db.to_csv(file_path, sep=';', index=False)
        logger.info(f"Dados salvos em {file_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar os dados: {e}")
        raise
        

def verifica_colunas_box_office(df_item):
    colunas_box_office = ["box_office_Domestic Box Office", "box_office_International Box Office", "box_office_Worldwide Box Office"]
    for coluna in colunas_box_office:
        if isinstance(df_item[coluna], str) and df_item[coluna] != "":
            return True
    return False


def get_data():
    with Bar('Processando', max=len(df_copy), suffix='%(percent).1f%% - %(eta)ds') as bar:
        for index, item in df_copy.iterrows():
            print('\n')
            logger.info(
                f"[{index}/{len(df_copy)} E({len(exceptions)})] {item['originalTitle']}")
            
            if verifica_colunas_box_office(item):
                logger.info("Financial: dados já coletados")
                bar.next()
                continue
            
            # Coletando dados financeiros
            title = item['originalTitle'].replace("&", "and")
            title = title.replace("'", "")
            title = title.replace("!", "")
            title = title.replace(":", "")
            title = title.replace(",", "")
            title = title.replace("?", "")
            title = title.replace("·", " ")
            title = title.replace("(", "")
            title = title.replace(")", "")
            title = title.replace(".", "")
            
            # Verifica se pelo menos um dos dados financeiros é válido
            def is_box_office_valid(financial_data):
                if financial_data['Domestic Box Office'] != "n/a" and financial_data['Domestic Box Office'] != "N/A" and financial_data['Domestic Box Office'] is not None:
                    return True
                if financial_data['International Box Office'] != "n/a" and financial_data['International Box Office'] != "N/A" and financial_data['International Box Office'] is not None:
                    return True
                if financial_data['Worldwide Box Office'] != "n/a" and financial_data['Worldwide Box Office'] != "N/A" and financial_data['Worldwide Box Office'] is not None:
                    return True
                return False

            def get_box_office(title):
                url = sb.get_url_title(title)
                financial_data = sb.fetch_movie_financials(url)
                if financial_data and is_box_office_valid(financial_data):
                    for k, v in financial_data.items():
                        if v == "n/a" or v == "N/A":
                            v = None
                        insert(df_copy, k, index, v, prefix="box_office")
                    logger.info("Financial: info coletada")
                    return financial_data
                else:
                    logger.warning(f"Financial: nenhum dado válido retornado:\n{financial_data}")
                    return None

            def clean_title_with_regex(title):
                title = re.sub(r'[^a-zA-Z0-9\s]', '', title)
                title = re.sub(r'\s+', ' ', title)
                title = title.strip()
                return title

            try:
                result = get_box_office(title)
                if result is None:
                    if title.lower()[:4] == "the ":
                        title = title[4:]
                        logger.info(f"Tentando sem o the: {title}")
                        result = get_box_office(title)
                        if result is None:
                            title = clean_title_with_regex(title)
                            logger.info(f"Tentando com regex: {title}")
                            result = get_box_office(title)
                            if result is None:
                                title = title.replace(" ", "-")
                                logger.info(f"Tentando com -: {title}")
                                result = get_box_office(title)
                                if result is None:
                                    logger.warning("Financial: nenhum dado retornado")
                    else:
                        title = clean_title_with_regex(title)
                        logger.info(f"Tentando com regex: {title}")
                        result = get_box_office(title)
                        if result is None:
                            title = title.replace(" ", "-")
                            logger.info(f"Tentando com -: {title}")
                            result = get_box_office(title)
                            if result is None:
                                logger.warning("Financial: nenhum dado retornado")
                    
            except Exception as e:
                logger.error(
                    f"[{len(exceptions)}/{MAX_EXCEPTIONS}] Financial: erro ao coletar dados - {e}")
                exceptions.append(item['originalTitle'])

            if len(exceptions) >= MAX_EXCEPTIONS:
                logger.error(
                    "Máximo de exceções atingido. Encerrando o processo.")
                break

            if index % 50 == 0:
                logger.info("Salvando dados parciais...")
                save(df_copy, "movies_partial.csv")

            print('\n')
            bar.next()
        bar.finish()
        logger.info("Processamento concluído.")

    # Salvando os dados com nome 'movies_{timestamp}.csv'
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")
    file_name = f"movies_{timestamp}.csv"
    save(df_copy, file_name)


if __name__ == "__main__":
    get_data()
