import json
import logging
import os
from urllib import request
import dotenv

import pandas as pd

dotenv.load_dotenv()

logger = logging.getLogger(__name__)


def load_tsv_data(file_path):
    try:
        df = pd.read_csv(file_path, sep='\t')
        logger.info(f"{len(df)} linhas carregadas do arquivo {file_path}")
        logger.debug(df.head())
        return df
    except Exception as e:
        logger.error(f"Erro ao abrir aqruivos no Pandas: {e}")
        exit(1)


def filter_movies_after_year(df, year=1970):
    df = df[df['titleType'] == 'movie']
    logger.info(f"Filmes: {len(df)} linhas filtradas")
    df = df[df['isAdult'] != '1']
    logger.info(f"Filmes não adultos: {len(df)} linhas filtradas")
    df = df[df['startYear'] >= str(year)]
    logger.info(f"Filmes após {year}: {len(df)} linhas filtradas")
    logger.debug(df.head())
    return df


def filter_most_rated(df, min_votes=50000):
    df = df[df['numVotes'] >= min_votes]
    df.reset_index(drop=True, inplace=True)
    logger.info(f"Filmes mais votados: {len(df)} linhas filtradas")
    logger.debug(df.head())
    return df


def get_movie_info_from_omdb(title_id):
    try:
        request_url = f"https://www.omdbapi.com/?i={title_id}&apikey={os.getenv('OMDB_API_KEY')}"
        with request.urlopen(request_url) as response:
            if response.status != 200:
                raise Exception(f"Erro na requisição: {response.status}")
            data = response.read().decode('utf-8')
            data = json.loads(data)
            logging.debug(f"Dados recebidos da API: {data}")
            return data
    except Exception as e:
        logger.warning(f"Erro ao buscar informações do filme: {e}")
        raise


def transform_data(df):
    try:
        df.reset_index(drop=True, inplace=True)
        df['title'] = df['primaryTitle']
        df['year'] = df['startYear']
        df['rating'] = df['averageRating']
        df['votes'] = df['numVotes']
        df['id'] = df['tconst']
        df['genres'] = df['genres'].apply(lambda x: x.split(','))
        logger.info(f"Dados transformados: {df.head()}")
        return df
    except KeyError as e:
        logger.error(f"Erro ao transformar dados: {e}")
        raise
