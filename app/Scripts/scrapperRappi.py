import time
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import concurrent.futures

rappi_root = f'https://www.rappi.com.br/'
headers = {
    'User-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}



def extrair_html(url, header):
    site = requests.get(url, headers=header)
    soup = BeautifulSoup(site.content, 'html.parser')
    return soup


def getMercados():
    start_time = time.perf_counter()
    urlsRappi = []
    mercadosBuscados = set()
    pagina_mercados = extrair_html('https://www.rappi.com.br/lojas/tipo/supermercados', headers)
    container_mercados = pagina_mercados.find('div', attrs={"data-qa": "stores-container"})
    linksMercados = container_mercados.find_all('a', attrs={"data-qa": re.compile(r'store-card-')})

    for link in linksMercados:
        nome_mercado = link.find('h3').get_text()
        if nome_mercado not in mercadosBuscados:
            urlsRappi.append(link.get('href'))
            mercadosBuscados.add(nome_mercado)

    end_time = time.perf_counter()
    print(f"Tempo para buscar os links: {end_time - start_time} segundos")
    return urlsRappi


def buscar_produtos(url):
    start_time = time.perf_counter()

    urlDefault = rappi_root + url
    paginas = extrair_html(urlDefault, headers)
    leftMenus = paginas.find_all('ul', attrs={"data-qa": "corridor-list"})

    mercadoVinculado = paginas.find('h1', attrs={"data-qa": "store-name"}).text.strip()
    imagemMercado = paginas.find('img', alt=mercadoVinculado).get('src')

    produtosList = {
        'nome': [],
        'preco': [],
        'unidadeMedida': [],
        'descricao': [],
        'categoria': [],
        'subcategoria': [],
        'imagemProduto': [],
        'mercadoVinculado': [],
        'imagemMercado': []
    }

    if leftMenus:
        for leftMenu in leftMenus:
            for leftMenuHref in leftMenu.find_all('a'):
                newUrl = rappi_root + leftMenuHref.get('href')
                categoria = leftMenuHref.text.strip()

                try:
                    verMaisList = extrair_html(newUrl, headers).find_all(
                        attrs={'data-qa': re.compile(r'store-corridors-list-aisle.*')})

                    if verMaisList:
                        for verMais in verMaisList:
                            links = verMais.find('a').get('href')
                            urlVerMais = rappi_root + links

                            paginasVerMais = extrair_html(urlVerMais, headers).find_all(
                                attrs={'data-qa': re.compile(r'product-item.*')})

                            subcategoriaProdutos = verMais.find('h2', attrs={'data-qa': 'corridor-carrousel-title-slider'})
                            for pagina in paginasVerMais:
                                try:
                                    preco = pagina.find(attrs={'data-qa': 'product-price'}).text.strip()
                                    nome = pagina.find(attrs={'data-qa': 'product-name'}).text.strip()
                                    descricao = pagina.find(attrs={'data-qa': 'product-description'}).text.strip()
                                    imagem = pagina.find('img').get('src')

                                    if "/" in preco:
                                        valor, unidade = preco.split("/")
                                        valor = valor.replace("R$", "").replace(",", ".").strip()
                                    else:
                                        valor = preco.replace("R$", "").replace(",", ".").strip()
                                        unidade = None

                                    produtosList['preco'].append(valor)
                                    produtosList['unidadeMedida'].append(unidade)
                                    produtosList['nome'].append(nome)
                                    produtosList['descricao'].append(descricao)
                                    produtosList['imagemProduto'].append(imagem)
                                    produtosList['mercadoVinculado'].append(mercadoVinculado)
                                    produtosList['imagemMercado'].append(imagemMercado)
                                    produtosList['subcategoria'].append(subcategoriaProdutos)
                                    produtosList['categoria'].append(categoria)

                                except Exception as e:
                                    print(f"Erro ao buscar produto: {e}")
                                    continue
                except Exception as e:
                    print(f"Erro ao buscar a lista de produtos: {e}")
                    continue

    df = pd.DataFrame(produtosList)
    df.to_csv(f'rappi_{url.replace("/", "_")}.csv', encoding='utf-8', sep=';', index=False)

    end_time = time.perf_counter()
    print(f"Tempo para buscar os produtos {url}: {end_time - start_time} segundos")


if __name__ == '__main__':
    urlsRappi = getMercados()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(buscar_produtos, urlsRappi)
