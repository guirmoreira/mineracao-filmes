import requests
from bs4 import BeautifulSoup
import logging

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL_BASE = "https://www.the-numbers.com"


def get_url_title(title):
    base_url = URL_BASE + "/search?searchterm="
    movie_url_title = title.replace(" ", "+")
    full_url = f"{base_url}{movie_url_title}#tab=summary"

    return full_url


def fetch_movie_financials(url):
    
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    logger.info(f"Acessando URL: {url}")
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logger.error(f"Erro ao acessar a página: {response.status_code}")
        raise Exception(f"Erro ao acessar a página: {response.status_code}")
    
    soup = BeautifulSoup(response.text, 'html.parser')
    data = {
        "Domestic Box Office": None,
        "International Box Office": None,
        "Worldwide Box Office": None
    }
    
    # Encontrar a tabela com os dados financeiros
    finance_table = soup.find_all('table', id='movie_finances')
    if len(finance_table) > 1:
        finance_table = finance_table[-1]
    else:
        finance_table = finance_table[0] if finance_table else None
    if not finance_table:
        logger.warning("Tabela financeira não encontrada.")
        logger.info("Tentando encontrar na página de busca.")
        search_results = soup.find_all('div', id='page_filling_chart')
        if search_results:
            for result in search_results:
                if not result.find('h1'):
                    continue
                if result.find('h1').get_text(strip=True) != "Movies":
                    continue
                logger.info("Resultados de busca encontrados.")
                # Encontrar o primeiro link válido para um filme
                result_links = result.find_all('a')
                for link in result_links:
                    if 'href' in link.attrs and '/movie/' in link['href']:
                        movie_link = link['href']
                        data = fetch_movie_financials(URL_BASE+movie_link)
                        return data
  
        else:
            logger.error("Sem resultados de busca.")
            return data
        
    if finance_table:
        rows = finance_table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                for key in data:
                    if key in label:
                        data[key] = value

    metrics_data = soup.find('div', id='summary')
    if not metrics_data:
        logger.warning("Métricas de budget não encontradas.")
        return data
    if metrics_data:
        metrics_table = metrics_data.find_all('table')[1]
        if metrics_table:
            rows = metrics_table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True)
                    value = cells[1].get_text(strip=True)
                    data[label] = value
        else:
            logger.warning("Tabela de métricas não encontrada.")
            return data
        
    return data


