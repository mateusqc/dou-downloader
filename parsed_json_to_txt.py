import os
import json

GENERATION_ROOT_DIR = 'parsed_json_files'
ANO = '2021'
MES = '09'
# TODO - automatizar obtenção de arquivos a processar - por meio da árvore de diretórios?


def process_dict(data, file):
    dic_keys = data.keys()
    for key in dic_keys:
        file.write('-----------------------------------------------\n')
        file.write(key + '\n\n\n')
        if type(data[key]) is dict:
            file.write('---------->\n')
            process_dict(data[key], file)
        else:
            process_list(data[key], file)


def process_list(data, file):
    for item in data:
        file.write(item)
        file.write('\n-\n\n')


def write_to_txt_file(path, data):
    with open(path, 'w') as f:
        process_dict(data, f)
        f.close()


path_to_files = f'{GENERATION_ROOT_DIR}/{ANO}/{MES}/'
file_list = []

with os.scandir(path_to_files) as entries:
    for entry in entries:
        print(entry.name)
        file_list.append(entry.name)

for file_path in file_list:
    f = open(path_to_files + file_path)
    json_data = json.load(f)
    f.close()
    file_name = file_path.split('.')[0]
    print(file_name)
    write_to_txt_file(f'./txt_files/{ANO}/{MES}/{file_name}.txt', json_data)
