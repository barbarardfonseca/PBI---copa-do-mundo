import requests
from bs4 import BeautifulSoup
import os

# Define a pasta para salvar as imagens
dest_dir = "C:/Users/Bárbara/OneDrive/Documentos/estudos/PBI - copa do mundo/flags"

# Faça uma solicitação HTTP para o site
response = requests.get("https://www.significados.com.br/bandeiras-dos-paises/")

# Parse o conteúdo da resposta
soup = BeautifulSoup(response.content, "html.parser")

# Encontre todas as imagens no site
images = soup.find_all("img", {"src": True, "title": True})

# Baixe cada imagem
for image in images:
    # Obtenha o URL da imagem
    url = image["src"]
    name = image["title"]
    url = url.replace("?width=50&blur=10", "")
    # Baixe a imagem
    response = requests.get(url)

    # Salve a imagem na pasta de destino
    filename = os.path.basename(name)
    CompleteUrl = os.path.join(dest_dir, filename)
    CompleteUrl = os.path.normpath(CompleteUrl)
    with open(CompleteUrl, "wb") as f:
        # print(CompleteUrl,'join')
        f.write(response.content)

# Imprima uma mensagem de confirmação
print("Imagens baixadas com sucesso!")