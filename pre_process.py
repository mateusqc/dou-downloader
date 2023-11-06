import pandas as pd
import numpy as np
from tqdm.auto import tqdm
from datetime import datetime, timedelta
import re
import mmap
from utils import revert_date_srt



CSV_BASE_DIR = './csv_files'
PRE_PROCESS_CSV_BASE_DIR = './pre_process_files'
PRE_PROCESSED_DAYS = './pre_processed_days.txt'

def write_processed_date(date):
    with open(PRE_PROCESSED_DAYS, 'a') as file:
        file.write(f'{date}\n')
        file.close()

def find_date_in_processed_file(date):
    with open(PRE_PROCESSED_DAYS) as file:
        s = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        b = bytearray()
        b.extend(map(ord, f'{date}'))
        if s.find(b) != -1:
            return True
        else:
            return False

def remove_html_tags(text):
  pattern = re.compile('<.*?>')
  return re.sub(pattern, ' ', text)

def pre_process_text(text):
    new_text = text.replace('\n', ' ')
    if new_text[0] == '"':
        new_text = new_text[1:]
    if new_text[-1] == '"':
        new_text = new_text[:-1]
    return new_text

def process(dou_df,day):

    dou_df.title = dou_df.title.astype(str)
    dou_df.content = dou_df.content.astype(str)

    dou_df["ano"] = dou_df['pubDate'].apply(lambda x: x.split('/')[2]).astype(int)
    dou_df["mes"] = dou_df['pubDate'].apply(lambda x: x.split('/')[1]).astype(int)
    dou_df["dia"] = dou_df['pubDate'].apply(lambda x: x.split('/')[0]).astype(int)

    dou_df_filtered = dou_df[["pubName", "urlTitle", "title", "content","pubDate","uuid","ano","mes","dia"]]

    dou_df_filtered["title"] = dou_df_filtered["title"].apply(lambda x: pre_process_text(remove_html_tags(x.replace('\n', ' '))).lower())
    dou_df_filtered["content"] = dou_df_filtered["content"].apply(lambda x: pre_process_text(remove_html_tags(x.replace('\n', ' '))).lower())
    dou_df_filtered.to_csv(f'./{PRE_PROCESS_CSV_BASE_DIR}/{day}.csv',mode='a', index=False)
    write_processed_date(day)

def pre_process_csv(date_list):
    dates_reversed = [revert_date_srt(x) for x in date_list]    
    for day in dates_reversed:
        if find_date_in_processed_file(day):
            print(f"O dia {day} já foi pré processado")
        else:
            
            df_do1 = pd.read_csv(f'{CSV_BASE_DIR}/{day}-do1.csv', low_memory=False)
            df_do2 = pd.read_csv(f'{CSV_BASE_DIR}/{day}-do2.csv', low_memory=False)
            df_do3 = pd.read_csv(f'{CSV_BASE_DIR}/{day}-do3.csv', low_memory=False)
            dou_df = pd.concat([df_do1,df_do2,df_do3], ignore_index=True)
            process(dou_df,day)    
