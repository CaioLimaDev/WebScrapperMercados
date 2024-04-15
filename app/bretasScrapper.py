import re
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import concurrent.futures

options = webdriver.ChromeOptions()
options.headless = True
options.add_argument("--window-size=1920,1080")
options.add_argument('--ignore-certificate-errors')
options.add_argument('--allow-running-insecure-content')
options.add_argument("--disable-extensions")
options.add_argument("--proxy-server='direct://'")
options.add_argument("--proxy-bypass-list=*")
options.add_argument("--start-maximized")
options.add_argument('--disable-gpu')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--no-sandbox')

driver = webdriver.Chrome(options=options, )
url = 'https://www.bretas.com.br'
headers = {
    'User-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"}


def getMenu(url):
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    menu_element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, 'bretas-mega-menu-0-x-menuDrawerIcon')))

    if 'bretas-mega-menu-0-x-menuDrawerIsOpen' not in menu_element.get_attribute('class'):
        menu_element.click()
        driver.implicitly_wait(10)

    return driver


def loadPage(driver):
    html_content = driver.page_source
    soup = BeautifulSoup(html_content, 'html.parser')
    return soup


def getLinksFromMenu(driver, url):
    try:
        driver = getMenu(url)
        soup = loadPage(driver)
        linksMenu = soup.find_all('a', class_=re.compile(r'bretas-mega-menu-0-x-menuDepartmentLink null'))

        allLinks = []

        for link in linksMenu:
            link_href = link.get('href')
            if link_href:
                newUrl = url + link_href
                allLinks.append(newUrl)

        return allLinks

    except Exception as e:
        return ("Ocorreu um erro:", e)


def getStaticPage(url, header):
    driver.get(url)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    return soup


def getQuantidadePaginas(driver, url, headers):
    driver.get(url)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'bretas-bretas-components-0-x-tabRowContainer')))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        qtdPaginas_element = soup.find('span', class_='bretas-bretas-components-0-x-showingPages')

        if qtdPaginas_element and "NaN" not in qtdPaginas_element:
            qtdPaginas_text = qtdPaginas_element.get_text().strip()
            index = qtdPaginas_text.split()
            qtd = index[-1]
            return qtd
        else:
            print("Quantidade de páginas não encontrada. Tentando novamente...")
    except Exception as e:
        print("Erro ao obter quantidade de páginas:", e)
        time.sleep(5)


def scrape():
    produtosList = {'nome': [], 'preco': [], 'descricao': []}
    for link in getLinksFromMenu(driver, url):

        qtdPaginas = None
        while qtdPaginas is None:
            qtdPaginas = getQuantidadePaginas(driver, link, headers)
            if qtdPaginas is None:
                print("Aguardando para tentar novamente...")
                time.sleep(5)

        categoria_element = getStaticPage(link, headers).find('div',
                                                              class_='vtex-rich-text-0-x-wrapper vtex-rich-text-0-x-wrapper--categoryClass')
        categoriaItens = categoria_element.get_text().strip() if categoria_element else "Categoria não disponível"

        for page_number in range(1, int(qtdPaginas) + 1):
            urlPages = f"{link}?page={page_number}"
            print(urlPages)
            itens = getStaticPage(urlPages, headers).find_all('div',
                                                              class_='vtex-search-result-3-x-galleryItem vtex-search-result-3-x-galleryItem--normal vtex-search-result-3-x-galleryItem--default pa4')

            print(itens)
            for item in itens:
                nome_element = item.find('div', class_='bretas-bretas-components-0-x-WrapperProductName')
                nomeProduto = nome_element.get_text().strip() if nome_element else "Nome não disponível"

                preco_element = item.find('div', class_=re.compile(r'regular-price.*'))
                precoProduto = preco_element.get_text().strip() if preco_element else "Preço não disponível"

                produtosList['nome'].append(nomeProduto)
                produtosList['preco'].append(precoProduto)
                produtosList['descricao'].append(categoriaItens)

    df = pd.DataFrame(produtosList)
    df.to_csv(f'bretasScrapper.csv', encoding='utf-8', sep=';', index=False)

threadPool = concurrent.futures.ThreadPoolExecutor()
threadPool.submit(scrape)
driver.quit()

