import datetime
import logging
import re

from progress.bar import Bar

import data_extract_transform as det
import scrap_the_numbers as sb

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

exceptions = []
MAX_EXCEPTIONS = 100

df_movies = det.load_tsv_data("datasets/title.basics.tsv")
df_movies = det.filter_movies_after_year(df_movies, 1970)
df_ratings = det.load_tsv_data("datasets/title.ratings.tsv")
df_w_ratings = df_movies.merge(df_ratings, on='tconst', how='left')
df = det.filter_most_rated(df_w_ratings, 10000)

df = df.sort_values(by='numVotes', ascending=False)
df = df.reset_index(drop=True)

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


def get_data():
    with Bar('Processando', max=len(df_copy), suffix='%(percent).1f%% - %(eta)ds') as bar:
        for index, item in df_copy.iterrows():
            print('\n\n')
            logger.info(
                f"[{index}/{len(df_copy)} E({len(exceptions)})] {item['originalTitle']}")

            # Coletando dados do OMDB
            try:
                omdb_info = det.get_movie_info_from_omdb(item['tconst'])
                if omdb_info:
                    for k, v in omdb_info.items():
                        insert(df_copy, k, index, v, prefix="omdb")
                    logger.info("OMDB: info coletada")
                else:
                    logger.warning("OMDB: nenhum dado retornado")
            except Exception as e:
                logger.error(
                    f"[{len(exceptions)}/{MAX_EXCEPTIONS}] OMDB: erro ao coletar dados - {e}")
                exceptions.append(item['originalTitle'])

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

            def get_box_office(title):
                url = sb.get_url_title(title)
                financial_data = sb.fetch_movie_financials(url)
                if financial_data and None not in financial_data.values() and "n/a" not in financial_data.values() and len(financial_data) > 3:
                    for k, v in financial_data.items():
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

    # Salvando os dados
    save(df_copy, "movies.csv")


if __name__ == "__main__":
    get_data()
