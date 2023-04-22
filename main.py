import mmap
import csv
import requests
import json
from bs4 import BeautifulSoup as bs
import re
from time import time
from datetime import datetime
from datetime import timedelta
import uuid
import os
from gazpacho import Soup
from threading import Thread
from queue import Queue
from urllib.request import urlopen, Request
import pandas as pd
import socket
from urllib3.connection import HTTPConnection
from playwright.sync_api import sync_playwright

import random

HTTPConnection.default_socket_options = (
    HTTPConnection.default_socket_options + [
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        (socket.SOL_TCP, socket.TCP_KEEPIDLE, 45),
        (socket.SOL_TCP, socket.TCP_KEEPINTVL, 10),
        (socket.SOL_TCP, socket.TCP_KEEPCNT, 6)
    ]
)

random.seed(47)

MAIN_URL = "https://www.in.gov.br/leiturajornal?"
start_date = "01/11/2022"
end_date = "30/11/2022"
BASE_CSV_DIR = './csv_files/'
BASE_JSON_DIR = './json_files/'
BASE_HTML_DIR = './html_files/'

SECOES_DOU = ['do1', 'do2', 'do3']

CSV_COLS = [
    'pubName',
    'urlTitle',
    'numberPage',
    'subTitulo',
    'titulo',
    'title',
    'pubDate',
    'content',
    'editionNumber',
    'hierarchyLevelSize',
    'artType',
    'pubOrder',
    'hierarchyStr',
    'hierarchyList',
    'uuid',
    'content_full'
]


def generate_dates(start=start_date, end=end_date):
    start_dt = datetime.strptime(start, '%d/%m/%Y')
    end_dt = datetime.strptime(end, '%d/%m/%Y')
    interval = [start_dt + timedelta(days=x)
                for x in range(0, (end_dt-start_dt).days + 1)]

    dates_set = set()

    for date in interval:
        dates_set.add(date.strftime('%Y-%m-%d'))

    date_list = list(dates_set)
    date_list.sort(reverse=True)

    return date_list

def revert_date_srt(date_str, separator = "-"):
    splitted = date_str.split(separator)
    return splitted[-1] + separator + splitted[1] + separator + splitted[0]

def get_process_tracking_file_from_action(action):
    if action == 'diarios':
        return './download_diarios.txt'
    elif action == 'atos':
        return './download_atos.txt'
    elif action == 'csv':
        return './processamento_csv.txt'
    return ''

def write_processed_date(date, secao, action):
    file_path = get_process_tracking_file_from_action(action)
    with open(file_path, 'a') as file:
        file.write(f'{date}-{secao}\n')
        file.close()

def find_date_in_processed_file(date, secao, action):
    file_path = get_process_tracking_file_from_action(action)
    with open(file_path) as file:
        s = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        b = bytearray()
        b.extend(map(ord, f'{date}-{secao}'))
        if s.find(b) != -1:
            return True
        else:
            return False

def find_uuid_in_processed(id, path):
    with open(path, 'r') as file:
        s = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        b = bytearray()
        b.extend(map(ord, str(id)))
        if s.find(b) != -1:
            return True
        else:
            return False

def convert_atos_to_csv(date = '02-02-2022'):
    CSV_FILE_PATH = BASE_CSV_DIR + date + '.csv'
    csv_file = open(CSV_FILE_PATH, 'w')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(CSV_COLS)

    for jornal in SECOES_DOU:
        if find_date_in_processed_file(date, jornal, 'csv'):
            print(f'{date} - {jornal} - Já processado!')
            csv_file.close()
            return
        
        JSON_FILE_PATH = BASE_JSON_DIR + date + '-' + jornal + '.json'

        parsed_json = {}

        with open(JSON_FILE_PATH, 'r') as json_file:
            json_content = json_file.read()
            parsed_json = json.loads(json_content)
            json_file.close()

        atos = parsed_json["jsonArray"]


        total_atos = len(atos)

        if total_atos == 0:
            print(f'\t{date} - {jornal} - Sem publicações!')
            continue

        for ato in atos:
            content_full = get_html_from_pub_order(date, jornal, ato["uuid"])
            ato["content_full"] = content_full
            conteudo = convert_to_csv_row(ato)
            csv_writer.writerow(conteudo)

    csv_file.close()

def get_html_from_pub_order(date, jornal, id):
    HTML_ATOS_DIR_PATH = BASE_HTML_DIR + 'atos/' + date + '-' + jornal + '/'
    content = ''
    with open(HTML_ATOS_DIR_PATH + id + '.html') as file:
        content = file.read()
    return content

def get_num_samples_from_date(date = "06-04-2022"):
    reversed_date = revert_date_srt(date)
    atos_tributarios = pd.read_csv("./atos_dou_completo_dt_formatted.csv")
    num_atos = len(atos_tributarios[atos_tributarios['dataPublicacao'] == reversed_date])
    return num_atos

def fetch_atos_from_json(date = '02-02-2022', jornal = 'do1', queue = Queue(), sample_mode = False):
    if find_date_in_processed_file(date, jornal, 'atos'):
        print(f'{date} - {jornal} - Já processado!')
        return

    JSON_FILE_PATH = BASE_JSON_DIR + date + '-' + jornal + '.json'
    HTML_ATOS_DIR_PATH = BASE_HTML_DIR + 'atos/' + date + '-' + jornal + '/'

    parsed_json = {}

    with open(JSON_FILE_PATH, 'r') as json_file:
        json_content = json_file.read()
        parsed_json = json.loads(json_content)
        json_file.close()

    # print(parsed_json["jsonArray"])
    atos = parsed_json["jsonArray"]

    total_atos = len(atos)
    atos_para_processamento = 0

    if total_atos == 0:
        print(f'\t{date} - {jornal} - Sem publicações!')

    uuid_was_added = False

    for ato in atos:
        if "uuid" not in ato:
            ato["uuid"] = str(uuid.uuid4())
            uuid_was_added = True
        if not sample_mode:
            queue.put((ato, HTML_ATOS_DIR_PATH, f'{date} - {jornal} - {atos_para_processamento} de {total_atos}'))
            atos_para_processamento += 1
    
    parsed_json["jsonArray"] = atos

    if uuid_was_added:
        with open(JSON_FILE_PATH, 'w') as json_file:
            json.dump(parsed_json, json_file)
    
    if sample_mode and total_atos > 0:
        num_atos = get_num_samples_from_date(date)
        total_atos = num_atos*2 if num_atos > 0 else 5
        atos = random.choices(atos, k=total_atos)
        for ato in atos:
            queue.put((ato, HTML_ATOS_DIR_PATH, f'{date} - {jornal} - {atos_para_processamento} de {total_atos}'))
            atos_para_processamento += 1
        
        queue.join()
        has_unprocessed_registry = True

        while has_unprocessed_registry:
            has_unprocessed_registry = False
            atos_para_processamento = 0
            for ato in atos:
                if not find_uuid_in_processed(ato["uuid"], HTML_ATOS_DIR_PATH + 'processed.txt'):
                    queue.put((ato, HTML_ATOS_DIR_PATH, f'{date} - {jornal} - {atos_para_processamento} de {total_atos}'))
                    has_unprocessed_registry = True
                atos_para_processamento += 1
            queue.join()

def validate_atos_processed(date = '02-02-2022', jornal = 'do1', queue = Queue()):
    JSON_FILE_PATH = BASE_JSON_DIR + date + '-' + jornal + '.json'
    HTML_ATOS_DIR_PATH = BASE_HTML_DIR + 'atos/' + date + '-' + jornal + '/'

    parsed_json = {}

    with open(JSON_FILE_PATH, 'r') as json_file:
        json_content = json_file.read()
        parsed_json = json.loads(json_content)
        json_file.close()

    atos = parsed_json["jsonArray"]

    total_atos = len(atos)
    atos_para_processamento = 0

    if total_atos == 0:
        print(f'\t{date} - {jornal} - Sem publicações!')

    has_unprocessed_registry = False

    for ato in atos:
        if not find_uuid_in_processed(ato["uuid"], HTML_ATOS_DIR_PATH + 'processed.txt'):
            queue.put((ato, HTML_ATOS_DIR_PATH, f'{date} - {jornal} - {atos_para_processamento} de {total_atos}'))
            has_unprocessed_registry = True
        atos_para_processamento += 1
    
    return has_unprocessed_registry

def single_ato_to_file(ato, target_path = "", message_prefix = ""):
    id = ato["uuid"]
    target_file_path = target_path + id + '.html'
    processed_file_path = target_path + 'processed.txt'

    if os.path.exists(processed_file_path) and find_uuid_in_processed(id, processed_file_path):
        print(f'{message_prefix} -- Ato já obtido!')
        return
    
    print(f'{message_prefix} -->> Iniciado!')
    content_full_unparsed, found = fetch_ato_content(ato["urlTitle"])
    content_full = str(content_full_unparsed)
    
    if found:
        with open(target_file_path, 'w') as t_file:
            t_file.write(content_full)

        operation = 'a'
        if not os.path.exists(processed_file_path):
            operation = 'w'

        with open(processed_file_path, operation) as file:
            file.write(id+'\n')
        
        print(f'{message_prefix} <<-- Download concluído!')



def convert_to_csv_row(conteudo):
    row_list = []
    for col in CSV_COLS:
        if col in conteudo:
            row_list.append(conteudo[col])
        else:
            row_list.append("")
    return row_list


def fetch_ato_content(url_title):
    BASE_ATO_URL = "https://www.in.gov.br/web/dou/-/"
    # req_headers = {
    #     "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    #     "Connection": "keep-alive",
    #     "Accept": "text/html",
    #     "Referer": "https://www.in.gov.br/leiturajornal?data=04-04-2022&secao=do3"
    # }
    # response_html = requests.get(f'{BASE_ATO_URL}{url_title}', headers=req_headers, timeout=50).text
    response_html = request_get_html_plw(f'{BASE_ATO_URL}{url_title}')
    response_html.replace(os.linesep, '')
    soup = bs(response_html, 'html.parser')
    elements = soup.find_all(attrs={"class": "texto-dou"})
    if len(elements) == 1:
        return elements[0], True
    else:
        other_elements = soup.find({"id": "conteudo"})
        NOT_FOUND_STR = "O recurso requisitado não foi encontrado."
        if len(str(other_elements)) > 0 and NOT_FOUND_STR in str(other_elements):
            print('Conteúdo de ato inexistente ' + url_title)
            return "", True
        print('Erro ao obter ato de URL ' + url_title)
        return "", False

def fetch_all_pubs_dia(date, DEST_PATH = "", secao="do1"):
    if find_date_in_processed_file(date, secao, 'diarios'):
        print(f'{date} - {secao} - Já processado!')
        return

    URL_STR = f'{MAIN_URL}data={date}&secao={secao}'
    JSON_PATH = DEST_PATH.replace('html', 'json') + '.json'

    # r = Request(URL_STR)
    # r.add_header('User-Agent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36')
    # # r.add_header('Connection', 'keep-alive')
    # r.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7')

    base_html = request_get_html_plw(URL_STR)

    # with urlopen(r) as base_html, open(DEST_PATH, 'wb') as f, open(JSON_PATH, 'w') as f_json:
    with open(DEST_PATH, 'w') as f, open(JSON_PATH, 'w') as f_json:
        f.write(base_html)

    with open(DEST_PATH, 'r') as o_file, open(JSON_PATH, 'w') as f_json:
        content = o_file.read()
        split1 = content.split('<script id="params" type="application/json">')
        split2 = split1[1].split('</script>')
        f_json.write(split2[0])

    write_processed_date(date, secao, 'diarios')

    create_atos_dir(date, secao)

def request_get_html_plw(URL_STR, is_full_diario = False):
    html = ""
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(URL_STR, wait_until="load")
        if is_full_diario:
            page.waitForSelector("div.portlet-boundary_leituradou_")
        # page.once("load", lambda: print("page loaded!"))
        html = page.content()
        browser.close()
    return html

def create_atos_dir(date, jornal):
    path = get_html_atos_files_path(date, jornal)
    exists = os.path.exists(path)
    if not exists:
        os.makedirs(path)

def get_html_atos_files_path(date, jornal):
    return f'./html_files/atos/{date}-{jornal}'

class DiarioDownloaderWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # diarios
            DATE_LINE_SEP, FILE_PATH, secao = self.queue.get()
            try:
                # diarios
                print(f'{DATE_LINE_SEP} - {secao} -->> Iniciado!')
                fetch_all_pubs_dia(DATE_LINE_SEP, FILE_PATH, secao)
                print(f'{DATE_LINE_SEP} - {secao} <<-- Download concluído!')
                # write_processed_date()
            except Exception as e:
                print(e)
                with open('./error.txt', 'a') as error_file:
                    error_file.write(f'Erro! - {DATE_LINE_SEP} - {secao}\n\n')
            finally:
                self.queue.task_done()


class AtoDownloaderWorker(Thread):
    is_sample_mode = False

    def __init__(self, queue, sample_mode = False):
        Thread.__init__(self)
        self.queue = queue
        self.is_sample_mode = sample_mode

    def run(self):
        while True:
            # atos
            ato, HTML_ATOS_DIR_PATH, message_prefix = self.queue.get()

            try:
                # atos
                single_ato_to_file(ato, HTML_ATOS_DIR_PATH, message_prefix)
            except Exception as e:
                print(e)
                with open('./error.txt', 'a') as error_file:
                    error_file.write(f'Erro! - {message_prefix}\n{e}\n\n')
            finally:
                self.queue.task_done()

class CSVBuilderWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # csv
            DATE_LINE_SEP = self.queue.get()
            try:
                # csv
                print(f'{DATE_LINE_SEP} -->> Iniciado!')
                convert_atos_to_csv(DATE_LINE_SEP)
                print(f'{DATE_LINE_SEP} <<-- Download concluído!')
                
            except Exception as e:
                print(e)
                with open('./error.txt', 'a') as error_file:
                    error_file.write(f'Erro! - {DATE_LINE_SEP}')
            finally:
                self.queue.task_done()

def main_diarios():
    ts = time()
    dates_list = generate_dates()
    # dates_list = ["20/03/2023"]

    queue = Queue()

    for x in range(10):
        worker = DiarioDownloaderWorker(queue)
        worker.daemon = True
        worker.start()

    for date in dates_list:
        for secao in SECOES_DOU:
            DATE_LINE_SEP = revert_date_srt(date)
            FILE_PATH = BASE_HTML_DIR + f'{DATE_LINE_SEP}-{secao}'
            queue.put((DATE_LINE_SEP, FILE_PATH, secao))
            
    queue.join()  

    for date in dates_list:
            DATE_LINE_SEP = revert_date_srt(date)
            for secao in SECOES_DOU:
                FILE_PATH = BASE_HTML_DIR + f'{DATE_LINE_SEP}-{secao}'
                while not find_date_in_processed_file(DATE_LINE_SEP, secao, 'diarios'):
                    queue.put((DATE_LINE_SEP, FILE_PATH, secao))
                    queue.join()

    print('Processamento concluído!')
    print('Finalizado em ' + str(time() - ts))

def main_atos(sample_mode = False):
    ts = time()
    dates_list = generate_dates()
    # dates_list = ["20/03/2023"]

    queue = Queue()

    for x in range(20):
        worker = AtoDownloaderWorker(queue, sample_mode)
        worker.daemon = True
        worker.start()


    for date in dates_list:
        for secao in SECOES_DOU:
            DATE_LINE_SEP = revert_date_srt(date)
            fetch_atos_from_json(DATE_LINE_SEP, secao, queue, sample_mode)

    if not sample_mode:            
        queue.join()

        for date in dates_list:
            DATE_LINE_SEP = revert_date_srt(date)
            for secao in SECOES_DOU:
                while validate_atos_processed(DATE_LINE_SEP, secao, queue):
                    queue.join()
    

    for date in dates_list:
            DATE_LINE_SEP = revert_date_srt(date)
            for secao in SECOES_DOU:
                write_processed_date(DATE_LINE_SEP, secao, 'atos')
                
    print('Processamento concluído!')
    print('Finalizado em %s', time() - ts)

def main_csv():
    ts = time()
    # dates_list = generate_dates()
    dates_list = ["06/04/2022"]

    queue = Queue()

    for x in range(100):
        worker = CSVBuilderWorker(queue)
        worker.daemon = True
        worker.start()

    for date in dates_list:
        DATE_LINE_SEP = revert_date_srt(date)
        queue.put((DATE_LINE_SEP))
            
    queue.join()

    for date in dates_list:
            DATE_LINE_SEP = revert_date_srt(date)
            for secao in SECOES_DOU:
                write_processed_date(DATE_LINE_SEP, secao, 'csv')
                
    print('Processamento concluído!')
    print('Finalizado em %s', time() - ts)

if __name__ == '__main__':   
    # main_diarios()
    main_atos(True)
    # main_csv()