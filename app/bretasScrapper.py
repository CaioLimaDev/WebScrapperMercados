import re
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd

options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument('--ignore-ssl-errors')

def getMenu(url):
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    menu_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'bretas-mega-menu-0-x-menuDrawerIcon')))

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
    site = requests.get(url, headers=header)
    soup = BeautifulSoup(site.content, 'html.parser')
    return soup

def getQuantidadePaginas(driver, url, headers):
    driver.get(url)
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, 'bretas-bretas-components-0-x-tabRowContainer')))
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    return soup

driver = webdriver.Chrome(options=options)
url = 'https://www.bretas.com.br'
headers = {'User-agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"}

produtosList = {'nome': [], 'preco': [], 'descricao': []}
for link in getLinksFromMenu(driver, url):

    qtdPaginas = getQuantidadePaginas(driver, link, headers).find('span', class_='bretas-bretas-components-0-x-showingPages').get_text().strip()
    index = qtdPaginas.split()
    qtd = index[-1]

    categoria_element = getStaticPage(link, headers).find('div',class_='vtex-rich-text-0-x-wrapper vtex-rich-text-0-x-wrapper--categoryClass')
    categoriaItens = categoria_element.get_text().strip() if categoria_element else "Categoria não disponível"

    for page_number in range(1, int(qtd) + 1):
        urlPages = f"{link}?page={page_number}"
        itens = getStaticPage(urlPages,headers).find_all('div', class_='vtex-search-result-3-x-galleryItem vtex-search-result-3-x-galleryItem--normal vtex-search-result-3-x-galleryItem--default pa4')

        for item in itens:
            nome_element = item.find('div', class_='bretas-bretas-components-0-x-WrapperProductName')
            nomeProduto = nome_element.get_text().strip() if nome_element else "Nome não disponível"

            preco_element = item.find('div', class_=re.compile(r'regular-price.*'))
            precoProduto = preco_element.get_text().strip() if preco_element else "Preço não disponível"

            produtosList['nome'].append(nomeProduto)
            produtosList['preco'].append(precoProduto)
            produtosList['descricao'].append(categoriaItens)

driver.quit()
df = pd.DataFrame(produtosList)
df.to_csv(f'bretasScrapper.csv',encoding='utf-8', sep=';',index=False)