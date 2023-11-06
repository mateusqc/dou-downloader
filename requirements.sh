
# Instalar as dependências Python usando pip3
echo "Instalando as dependências Python..."
pip3 install -r "requirements.txt"

echo "Instalando os browsers necessários para o playwright"
playwright install
