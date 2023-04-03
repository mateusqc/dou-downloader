import requests
import json
from bs4 import BeautifulSoup as bs
import re
from datetime import datetime
from datetime import timedelta
import wget
import os

MAIN_URL = "http://www.diario.ac.gov.br/"
start_date = "01/02/2022"
end_date = "02/02/2022"
BASE_JSON_ID_DIR = './json_files/by_id/'
BASE_JSON_MESANO_DIR = './json_files/by_mesano/'
BASE_PDF_DIR = './pdf_files/'
BASE_HTML_DIR = './pdf_files/'


def generate_dates(start=start_date, end=end_date):
    start_dt = datetime.strptime(start, '%d/%m/%Y')
    end_dt = datetime.strptime(end, '%d/%m/%Y')
    interval = [start_dt + timedelta(days=x)
                for x in range(0, (end_dt-start_dt).days + 1)]

    dates_set = set()

    for date in interval:
        dates_set.add(date.strftime('%d-%m-%Y'))

    date_list = list(dates_set)
    date_list.sort()

    for date in date_list:
        splitted_date = date.split('-')
        print(splitted_date[1]+'/'+splitted_date[0])


def get_id_from_title(title_text=''):
    splitted = title_text.split()
    for text in splitted:
        if re.search("^\d*$", text):
            return text


def export_json_files(mesano, dic_json_content={}):
    export_mesano_json(mesano, dic_json_content)
    pub_ids = dic_json_content.keys()
    for pub_id in pub_ids:
        export_id_json(pub_id, dic_json_content[pub_id])


def export_mesano_json(mesano, json_content):
    replaced_mes_ano = mesano.replace('/', '-')
    with open(BASE_JSON_MESANO_DIR + replaced_mes_ano + '.json', 'w') as outfile:
        json.dump(json_content, outfile)


def export_id_json(id, json_content):
    with open(BASE_JSON_ID_DIR + id + '.json', 'w') as outfile:
        json.dump(json_content, outfile)


def download_pdf_file(file_metadata):
    splitted_mesano = file_metadata["mesano"].split('/')
    mes = splitted_mesano[0]
    ano = splitted_mesano[1]

    if not os.path.exists(BASE_PDF_DIR + ano):
        os.mkdir(BASE_PDF_DIR + ano)

    if not os.path.exists(BASE_PDF_DIR + ano + '/' + mes):
        os.mkdir(BASE_PDF_DIR + ano + '/' + mes)

    full_working_dir = BASE_PDF_DIR + ano + '/' + mes + '/'

    wget.download(file_url, full_working_dir + file_metadata["file_name"])


# mesano_list = generate_dates()
mesano_list = ["09/2021"]

for mesano in mesano_list:
    data_by_id = {}

    data = {
        "mesano": mesano
    }
    response_html = requests.post(MAIN_URL, data).text
    response_html.replace('\n', '').replace('\t', '')

    soup = bs(response_html, 'html.parser')

    for a in soup.find_all(attrs={"title": "Fazer download"}):
        file_url = a["href"]
        file_original_title = a.text.strip()
        id = get_id_from_title(file_original_title)

        print(a["href"])
        print(a.text.strip())
        dic = {
            "id": id,
            "file_url": file_url,
            "file_original_title": file_original_title,
            "mesano": mesano
        }

        if id not in data_by_id.keys():
            data_by_id[id] = []
        else:
            dic["id"] = file_original_title.split()[-1]

        dic["file_name"] = dic["id"] + '.pdf'
        data_by_id[id].append(dic)

        download_pdf_file(dic)

    print(data_by_id)
    export_json_files(mesano, data_by_id)
