from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
from bs4 import BeautifulSoup

options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument('--ignore-ssl-errors')

driver = webdriver.Chrome(options=options)
url = 'https://www.bretas.com.br'
headers = {'User-agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"}

def seleniumRequest(driver, url):
    driver.get(url)
    menu_element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, 'bretas-mega-menu-0-x-menuDrawerIcon')))

    if 'bretas-mega-menu-0-x-menuDrawerIsOpen' not in menu_element.get_attribute('class'):
        menu_element.click()
        driver.implicitly_wait(10)

    return driver.page_source


def scrapeBretas():
    html_content = seleniumRequest(driver, url)
    driver.quit()
    soup = BeautifulSoup(html_content, 'html.parser')
    return soup


def getLinksFromMenu():
    try:
        soup = scrapeBretas()
        itensMenu = soup.find_all('div', class_='bretas-mega-menu-0-x-menuDepartmentWrapper')

        for item in itensMenu:
            link = item.find('a')['href']
            newUrl = url + link
            return(newUrl)

    except Exception as e:
        return ("Ocorreu um erro:", e)

def requisicao(url, header):
    site = requests.get(url, headers=header)
    soup = BeautifulSoup(site.content, 'html.parser')
    return soup

for link in getLinksFromMenu():
    pagina = requisicao(link,headers)
    print(pagina)