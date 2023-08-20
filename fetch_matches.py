import mmap
from bs4 import BeautifulSoup as bs
from time import time, sleep
import os
# from gazpacho import Soup
from threading import Thread
from queue import Queue
import pandas as pd
from playwright.sync_api import sync_playwright
import numpy as np
import urllib.request

processed_file_path = './matches_files/downloaded_uuids.txt'

def find_uuid_in_processed(id, path = processed_file_path):
    try:
        with open(path, 'r') as file:
            s = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
            b = bytearray()
            b.extend(map(ord, str(id)))
            if s.find(b) != -1:
                return True
            else:
                return False
    except:
        print("Erro ao ler arquivo: " + path)
        return False

def write_processed_uuid(ato_uuid, path = processed_file_path):
    with open(path, 'a') as file:
        file.write(f'{ato_uuid}\n')
        file.close()

def request_get_html_plw(URL_STR):
    html = ""
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(URL_STR, wait_until="load", timeout = 0)
        # page.once("load", lambda: print("page loaded!"))
        html = page.content()
        browser.close()
    return html


def request_get_html(URL_STR):
    html = ""
    with urllib.request.urlopen(URL_STR, timeout=15) as response:
        html = response.read().decode('utf-8')
    return html

def fetch_ato_content(url_title):
    BASE_ATO_URL = "https://www.in.gov.br/web/dou/-/"
    # response_html = request_get_html_plw(f'{BASE_ATO_URL}{url_title}')
    response_html = request_get_html(f'{BASE_ATO_URL}{url_title}')
    response_html.replace(os.linesep, '')
    soup = bs(response_html, 'html.parser')
    elements = soup.find_all(attrs={"class": "texto-dou"})
    if len(elements) == 1:
        return elements[0], True
    else:
        NOT_FOUND_STR = "O recurso requisitado não foi encontrado."
        if len(str(soup)) > 0 and NOT_FOUND_STR in str(soup):
            print('Conteúdo de ato inexistente ' + url_title)
            return "", True
        print('Erro ao obter ato de URL ' + url_title)
        return "", False

def attempt_download(ato_uuid, ato_url):
    target_file_path = "./matches_files/downloaded/" + ato_uuid + '.html'
    content_full_unparsed, found = fetch_ato_content(ato_url)
    content_full = str(content_full_unparsed)
    
    if found:
        with open(target_file_path, 'w') as t_file:
            t_file.write(content_full)

        operation = 'a'
        if not os.path.exists(processed_file_path):
            operation = 'w'

        with open(processed_file_path, operation) as file:
            file.write(ato_uuid+'\n')
        


class AtoDownloaderWorker(Thread):
    worker_num = None

    def __init__(self, queue, worker_num):
        Thread.__init__(self)
        self.queue = queue
        self.worker_num = worker_num

    def run(self):
        while True:
            # atos
            ato_uuid, ato_url, total_count, total_size, slice_num, total_slices = self.queue.get()
            percentage = total_count/total_size * 100

            try:
                if not find_uuid_in_processed(ato_uuid):
                    print(f"[W_{'%2d' % self.worker_num}] {total_count}/{total_size} ({'%.2f' % percentage}%) - {slice_num}/{total_slices} - {ato_uuid} -->> Download INICIADO!")
                    attempt_download(ato_uuid, ato_url)
                    print(f"[W_{'%2d' % self.worker_num}] {total_count}/{total_size} ({'%.2f' % percentage}%) - {slice_num}/{total_slices} - {ato_uuid} <<-- Download concluído!")
                else:
                    print(f"[W_{'%2d' % self.worker_num}] {total_count}/{total_size} ({'%.2f' % percentage}%) - {slice_num}/{total_slices} - {ato_uuid} - Já processado!")
            except Exception as e:
                print(f"[W_{'%2d' % self.worker_num}] ############## ERRO!\n", e)
                with open('./error.txt', 'a') as error_file:
                    error_file.write(f'Erro! - {ato_uuid} - {ato_url}\n{e}\n\n')
            finally:
                self.queue.task_done()

def parse_processed_uuids():
    newfile_path = './matches_files/downloaded_uuids_np.txt'
    data = ""
    with open(processed_file_path, 'r') as file:
        data = file.read().strip().replace('\n', ',')
    with open(newfile_path, 'w') as file:
        file.write(data)
    return np.loadtxt(newfile_path, dtype='str',delimiter=',')


def fetch_atos():
    start_index = 262910
    finish_index = 525819
    slice_num = 2
    total_slices = 6

    ts = time()

    queue = Queue()

    for x in range(40):
        worker = AtoDownloaderWorker(queue, x)
        worker.daemon = True
        worker.start()

    print(" -> Lendo CSV...")
    atos_df = pd.read_csv("./matches_files/dou_filtrado_p_download.csv", low_memory=False)
    print(" -> CSV lido com sucesso!")

    is_finished = False
    total_size = (finish_index - start_index) + 1
    total_count = 0
    curr_count = 0
    curr_limit = 14000


    print(" -> INICIANDO PROCESSAMENTO")
    while not is_finished:
        is_finished = True

        print(" -> Lendo UUIDs já processados...")
        processed_uuids = parse_processed_uuids()
        print(f" -> UUIDs já processados lidos com sucesso! -> {len(processed_uuids)}")

        filtered_df = atos_df.iloc[start_index:finish_index].copy()
        filtered_df = filtered_df[~filtered_df["uuid"].isin(processed_uuids)]
        
        total_count = total_size - len(filtered_df)

        for idx in filtered_df.index:
            total_count += 1

            ato_uuid = filtered_df.loc[idx]["uuid"]
            
            if not find_uuid_in_processed(ato_uuid):
                is_finished = False
                curr_count += 1
                queue.put((ato_uuid, filtered_df.loc[idx]["urlTitle"], total_count, total_size, slice_num, total_slices))
            else:
                percentage = total_count/total_size * 100
                print(f"{total_count}/{total_size} ({'%.2f' % percentage}%) - {slice_num}/{total_slices} - {ato_uuid} - Já processado!")

            if curr_count >= curr_limit:
                queue.join()
                break

        print("Aguardando 5 min para evitar bloqueio de requisição...")
        sleep(300)
        curr_count = 0
        total_count = 0
        
    print(f"{total_count}/{total_size} processados.")
    print('Processamento concluído!')
    print('Finalizado em ' + str(time() - ts) + 's')

if __name__ == '__main__':   
    fetch_atos()
    # main_csv()