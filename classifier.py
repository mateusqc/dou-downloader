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

def predict(model, texts, tokenizer):
  inputs = tokenizer(texts, return_tensors='pt', padding=True, truncation=True, max_length=max_length).to(device)
  output = model(**inputs)
  return torch.argmax(output.logits, 1), F.softmax(output.logits, dim=1)

tokenizer = BertTokenizer.from_pretrained('flavio-nakasato/berdou_500k', do_lower_case=False)
model = BertForSequenceClassification.from_pretrained('trained-models/trib-imbalanced-berdou-06-04-23',
                                                      num_labels=nclasses,
                                                      output_attentions = False,
                                                      output_hidden_states = False).to(device)
model.cuda()

pred_labels, pred_probs = predict(model, 'ato declaratório executivo cosit 43, de 5 de abril de 2022 enquadra veículo em " ex " da tipi a coordenadora - geral de tributação, no uso da atribuição que lhe confere o inciso ii do art. 358 do regimento interno da secretaria da receita federal do brasil, aprovado pela portaria me 284, de 27 de julho de 2020, tendo em vista o disposto na nota complementar nc ( 87 - 1 ) da tabela de incidência do imposto sobre produtos industrializados ( tipi ), aprovada pelo decreto 8. 950, de 29 de dezembro de 2016, e na instrução normativa 929, de 25 de março de 2009, alterada pela instrução normativa 1. 734, de 01 de setembro de 2017, e ainda o que consta do processo 10265. 029083 / 2022 - 85, declara : art. o veículo relacionado no anexo único a este ato declaratório executivo cumpre as exigências para enquadramento no ex 02 do código 8702. 10. 00 da', tokenizer)

print(pred_labels, pred_probs)

print(f"{pred_probs[0][1].item():.8f}")