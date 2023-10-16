import pandas as pd
import numpy as np
from tqdm.auto import tqdm

BASE_PATH_NEW_YODA = './processando_dados_yoda/dados'
BASE_PATH_NEW_DOU = './full_data_dou'
BASE_PATH = './data'

import random
import nltk
nltk.download('punkt')

import torch
import torch.nn as nn
from torch import optim
from transformers import AutoTokenizer, AutoModelForSequenceClassification, BertTokenizer, BertForSequenceClassification
from sklearn.metrics import accuracy_score, f1_score, recall_score, precision_score, confusion_matrix
from torch.utils.data import DataLoader
import seaborn as sns
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import torch.nn.functional as F
import oracledb
from utils import revert_date_srt


PRE_PROCESS_CSV_BASE_DIR = './pre_process_files'


seed_val = 42

random.seed(seed_val)
np.random.seed(seed_val)
torch.manual_seed(seed_val)
torch.cuda.manual_seed_all(seed_val)


device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

nclasses = 2
nepochs = 4
batch_size = 8
batch_status = 32
learning_rate = 2e-5
max_length = 512
model_name = "trib-all-imbalanced-berdou-lr--"


print(device)

torch.cuda.empty_cache()

def predict(model, texts, tokenizer):
  inputs = tokenizer(texts, return_tensors='pt', padding=True, truncation=True, max_length=max_length).to(device)
  output = model(**inputs)
  return  F.softmax(output.logits, dim=1)

tokenizer = BertTokenizer.from_pretrained('flavio-nakasato/berdou_500k', do_lower_case=False)
model = BertForSequenceClassification.from_pretrained('trained-models/trib-imbalanced-bert-0.9989976409435456',
                                                      num_labels=nclasses,
                                                      output_attentions = False,
                                                      output_hidden_states = False).to(device)
model.cuda()

connection = oracledb.connect(
    user="dou",
    password="dou",
    dsn="localhost/FREEPDB1")

print("Successfully connected to Oracle Database")

def classify_by_day(df):
  cursor = connection.cursor()
  sql = "INSERT INTO DOU_ATOS (PUB_NAME, URL_TITLE, TITLE, CONTENT, PUB_DATE, UUID, ANO, DIA, MES, LABEL) VALUES (:1, :2, :3, :4, to_date(:5,'dd/MM/yyyy'), :6, :7, :8, :9, :10)" 
  df.content = df.content.astype(str)
  for row in df.itertuples():
    # df.at[row.Index,'label'] = "{:.10f}".format(predict(model,str(row.content),tokenizer)[0][1].item())
    label = "{:.10f}".format(predict(model,str(row.content),tokenizer)[0][1].item())
    #row['label'] = predict(model,str(row.content),tokenizer)[0][1].item()
    cursor.execute(sql,(row.pubName, row.urlTitle , row.title, row.content, row.pubDate, row.uuid, row.ano, row.mes, row.dia, label))
    connection.commit()    
  connection.close()
  

def classify(date_list):
  for d in date_list:
    day = revert_date_srt(d)
    test_df = pd.read_csv(f"./{PRE_PROCESS_CSV_BASE_DIR}/{day}.csv")
    classify_by_day(test_df)
