## Iniciando o banco de dados

docker run -d -p 1521:1521 -e ORACLE_PASSWORD=123456 -v oracle-volume:/opt/oracle/oradata gvenzl/oracle-free

Rodar os Scripts de banco.

Executar o script: python3 main.py '29/09/2023'