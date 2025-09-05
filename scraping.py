from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import json

HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://www.reclameaqui.com.br/',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Connection': 'keep-alive',
        'sec-ch-ua-platform': 'Windows'
    }


def get_json(company_id, page=0):
    """Fetch the content of a web page."""
    
    url = f"https://iosearch.reclameaqui.com.br/raichu-io-site-search-v1/query/companyComplains/10/{page}?company={company_id}"

    try:
        response = Request(url, headers=HEADERS)
        html = urlopen(response).read().decode('utf-8')
        response_json = json.loads(html)
        return response_json['complainResult']['complains']
    except Exception as e:
        raise RuntimeError(f"Error fetching page: {e}")


def get_all_data(company_id = "ABfFz9T5OFF-h9d4"):
    data = get_json(company_id)
    n_data = data['count']
    all_data = []
    problems = set()

    for page in range(0, n_data, 10):
        json_data = get_json(company_id, page)
        all_data.extend(json_data['data'])
        problems.update((item['name'], item['count']) for item in json_data['problems'])
    
    return all_data, problems


def fetch_html(url):
    try:
        response = Request(url, headers=HEADERS)
        html = urlopen(response).read().decode('utf-8')
        return html
    except Exception as e:
        raise RuntimeError(f"Error fetching HTML: {e}")


def get_full_description(reclamacao):
    if 'descriptionMasked' in reclamacao and reclamacao['descriptionMasked']:
        return reclamacao['descriptionMasked']
    else:
        url = f"https://www.reclameaqui.com.br/{reclamacao['companyShortname']}/{reclamacao['url']}/"
        html = fetch_html(url)
        soup = BeautifulSoup(html, 'html.parser')

        description_div = soup.find('p', attrs={'data-testid': 'complaint-description'})

        if description_div:
            return description_div.get_text(strip=True)
        else:
            return "No description available"