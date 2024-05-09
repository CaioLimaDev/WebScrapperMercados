import time
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import concurrent.futures

rappi_root = f'https://www.rappi.com.br/'
headers = {
    'User-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}

mercados_a_excluir = ["pao-de-azucar-superfresh", "carrefour-big-hiper"]

def extrair_html(url, header):
    site = requests.get(url, headers=header)
    soup = BeautifulSoup(site.content, 'html.parser')
    return soup


def getMercados():
    mercados = {
        'nomeMercado': [],
        'imagemMercado': []
    }
    urlsRappi = []
    mercadosBuscados = set()
    pagina_mercados = extrair_html('https://www.rappi.com.br/lojas/tipo/supermercados', headers)
    container_mercados = pagina_mercados.find('div', attrs={"data-qa": "stores-container"})
    linksMercados = container_mercados.find_all('a', attrs={"data-qa": re.compile(r'store-card-')})

    for link in linksMercados:
        nome_mercado = link.find('h3').get_text()
        imagem_mercado = link.find('img').get('src')

        if nome_mercado not in mercados_a_excluir:
            mercados['nomeMercado'].append(nome_mercado)
            mercados['imagemMercado'].append(imagem_mercado)

            if nome_mercado not in mercadosBuscados:
                urlsRappi.append(link.get('href'))
                mercadosBuscados.add(nome_mercado)

    mr = pd.DataFrame(mercados)
    mr.to_csv(f'mercados.csv', encoding='utf-8', sep=';', index=False)

    return urlsRappi


def buscar_produtos(url):
    start_time = time.perf_counter()

    urlDefault = rappi_root + url
    paginas = extrair_html(urlDefault, headers)
    leftMenus = paginas.find_all('ul', attrs={"data-qa": "corridor-list"})

    mercadoVinculado = paginas.find('h1', attrs={"data-qa": "store-name"}).text.strip()

    produtosList = {
        'nome': [],
        'preco': [],
        'descricao': [],
        'imagemProduto': [],
        'mercadoVinculado': [],
    }

    if leftMenus:
        for leftMenu in leftMenus:
            for leftMenuHref in leftMenu.find_all('a'):
                newUrl = rappi_root + leftMenuHref.get('href')

                try:
                    verMaisList = extrair_html(newUrl, headers).find_all(
                        attrs={'data-qa': re.compile(r'store-corridors-list-aisle.*')})

                    if verMaisList:
                        for verMais in verMaisList:
                            links = verMais.find('a').get('href')
                            urlVerMais = rappi_root + links

                            paginasVerMais = extrair_html(urlVerMais, headers).find_all(
                                attrs={'data-qa': re.compile(r'product-item.*')})

                            for pagina in paginasVerMais:
                                try:
                                    preco = pagina.find(attrs={'data-qa': 'product-price'}).text.strip()
                                    nome = pagina.find(attrs={'data-qa': 'product-name'}).text.strip()
                                    descricao = pagina.find(attrs={'data-qa': 'product-description'}).text.strip()
                                    imagem = pagina.find('img').get('src')

                                    produtosList['preco'].append(preco)
                                    produtosList['nome'].append(nome)
                                    produtosList['descricao'].append(descricao)
                                    produtosList['imagemProduto'].append(imagem)
                                    produtosList['mercadoVinculado'].append(mercadoVinculado)
                                except Exception as e:
                                    print(f"Erro ao buscar produto: {e}")
                                    continue
                except Exception as e:
                    print(f"Erro ao buscar a lista de produtos: {e}")
                    continue

    df = pd.DataFrame(produtosList)
    df.to_csv(f'rappi_{url.replace("/", "_")}.csv', encoding='utf-8', sep=';', index=False)

    end_time = time.perf_counter()
    print(f"Tempo para buscar o mercado {url}: {end_time - start_time} segundos")


if __name__ == '__main__':
    urlsRappi = getMercados()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(buscar_produtos, urlsRappi)
