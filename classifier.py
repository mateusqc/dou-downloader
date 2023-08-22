import pandas as pd
import math
import numpy as np
import re
from tqdm.auto import tqdm
import html

BASE_PATH_NEW_YODA = './processando_dados_yoda/dados'
BASE_PATH_NEW_DOU = './full_data_dou'
BASE_PATH = './data'

import csv
import random
import nltk
nltk.download('punkt')

import os
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
from sklearn.metrics import precision_recall_curve, auc
from matplotlib import pyplot as plt
from utils import revert_date_srt


PRE_PROCESS_CSV_BASE_DIR = './pre_process_files'


seed_val = 42

random.seed(seed_val)
np.random.seed(seed_val)
torch.manual_seed(seed_val)
torch.cuda.manual_seed_all(seed_val)


device = torch.device('cpu')
nclasses = 2
nepochs = 4
batch_size = 8
batch_status = 32
learning_rate = 2e-5
max_length = 512
model_name = "trib-all-imbalanced-berdou-lr--"


print(device)

def predict(model, texts, tokenizer):
  inputs = tokenizer(texts, return_tensors='pt', padding=True, truncation=True, max_length=max_length).to(device)
  output = model(**inputs)
  return  F.softmax(output.logits, dim=1)

tokenizer = BertTokenizer.from_pretrained('flavio-nakasato/berdou_500k', do_lower_case=False)
model = BertForSequenceClassification.from_pretrained('trained-models/trib-imbalanced-berdou-06-04-23',
                                                      num_labels=nclasses,
                                                      output_attentions = False,
                                                      output_hidden_states = False).to(device)
# model.cuda()

def classify_by_day(df):
  df.content = df.content.astype(str)
  for row in df.itertuples():
    df.at[row.Index,'label'] = predict(model,str(row.content),tokenizer)[0][1].item()
  print(df[['content','label']].head(50))

def classify(date_list):
  for d in date_list:
    day = revert_date_srt(d)
    test_df = pd.read_csv(f"./{PRE_PROCESS_CSV_BASE_DIR}/{day}.csv")
    classify_by_day(test_df)
