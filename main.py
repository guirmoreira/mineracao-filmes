import datetime
import logging

import data_extract_transform as det
import scrap_boxoffice as sb

from progress.bar import Bar

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

exceptions = []
MAX_EXCEPTIONS = 20

df_movies = det.load_tsv_data("title.basics.tsv")
df_movies = det.filter_movies_after_year(df_movies, 1970)
df_ratings = det.load_tsv_data("title.ratings.tsv")
df_w_ratings = df_movies.merge(df_ratings, on='tconst', how='left')
df = det.filter_most_rated(df_w_ratings, 50000)

df = df.sort_values(by='numVotes', ascending=False)
df = df.reset_index(drop=True)
df = df[:1000]

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
            logger.info(f"[{index}/{len(df_copy)} E({len(exceptions)})] {item['originalTitle']}")

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

            url = sb.get_url_title(title)
            try:
                financial_data = sb.fetch_movie_financials(url)
                if financial_data:
                    for k, v in financial_data.items():
                        insert(df_copy, k, index, v, prefix="box_office")
                    logger.info("Financial: info coletada")
                else:
                    logger.warning("Financial: nenhum dado retornado")
            except Exception as e:
                logger.error(
                    f"[{len(exceptions)}/{MAX_EXCEPTIONS}] Financial: erro ao coletar dados - {e}")
                exceptions.append(item['originalTitle'])

            if len(exceptions) >= MAX_EXCEPTIONS:
                logger.error("Máximo de exceções atingido. Encerrando o processo.")
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

