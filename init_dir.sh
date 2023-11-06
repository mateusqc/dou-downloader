#!/bin/bash

path=$(pwd)

csv="$path""/csv_files"

html="$path""/html_files"

json="$path""/json_files"

pre_processed="$path""/pre_process_files"

# Csv
if [ -d "$csv" ]; then
    echo "O diretório '$csv' já existe."
else
    mkdir $csv
    echo "O diretório '$csv' foi criado."
fi

# JSON
if [ -d "$json" ]; then
    echo "O diretório '$json' já existe."
else
    mkdir $json
    echo "O diretório '$json' foi criado."
fi

# HTML
if [ -d "$html" ]; then
    echo "O diretório '$html' já existe."
else
    mkdir $html
    echo "O diretório '$html' foi criado."
fi

# pre-processado
if [ -d "$pre_processed" ]; then
    echo "O diretório '$pre_processed' já existe."
else
    mkdir $pre_processed
    echo "O diretório '$pre_processed' foi criado."
fi
