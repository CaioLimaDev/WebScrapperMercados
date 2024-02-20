import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import math

urlsRappi=['lojas/900315281-pao-de-azucar','lojas/900037048-moreira','900036589-carrefour-hiper-super-market']
urlRappi = f'https://www.rappi.com.br/'
headers = {'User-agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"}

def requisicao(url, header):
    site = requests.get(url, headers=header)
    soup = BeautifulSoup(site.content, 'html.parser')
    return soup

for url in urlsRappi:
    urlDefault = urlRappi+url
    leftMenus = requisicao(urlDefault, headers).find('ul', attrs={"data-qa": "corridor-list"})

    produtosList = {'nome':[], 'preco':[], 'descricao':[]}

    if leftMenus:
        for leftMenu in leftMenus.find_all('li'):
            leftMenuHref = leftMenu.find('a').get('href')
            newUrl = urlRappi+leftMenuHref

            verMaisList = requisicao(newUrl,headers).find_all(attrs={'data-qa': re.compile(r'store-corridors-list-aisle.*')})

            for verMais in verMaisList:
                links = verMais.find('a').get('href')
                urlVerMais = urlRappi+links

                paginasVerMais = requisicao(urlVerMais, headers).find_all(attrs={'data-qa': re.compile(r'product-item.*')})

                for pagina in paginasVerMais:
                    preco = pagina.find(attrs={'data-qa': 'product-price'}).text.strip()
                    nome = pagina.find(attrs={'data-qa': 'product-name'}).text.strip()
                    descricao = pagina.find(attrs={'data-qa': 'product-description'}).text.strip()

                    produtosList['preco'].append(preco)
                    produtosList['nome'].append(nome)
                    produtosList['descricao'].append(descricao)

    df = pd.DataFrame(produtosList)
    df.to_csv(f'rappi_{url.replace("/", "_")}.csv', encoding='utf-8', sep=';', index=False)