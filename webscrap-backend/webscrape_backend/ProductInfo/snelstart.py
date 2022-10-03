import requests
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import re
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from webscrape_backend.hashfile import HashMap

endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "visma_productInfo"
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)

def storeToAztable_snelstart():

    itemId = f"Item{5}"
    data = mergeAllData(uid=itemId)
    container.upsert_item(data)


def snelstart_ondernemers():

    url = 'https://www.snelstart.nl/accountant/pakketten-voor-klanten'
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    soup = BeautifulSoup(html, 'html.parser')
    product_title = []
    product_price = []
    users = []
    administraties = []
    ondernemers_snelstart_info = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    product_info = soup.find_all(
        'div', {'class': 'pricing-box pricing-minimal text-center'})
    for div_tag in product_info:
        products = div_tag.find_all(
            'div', {'class': 'pricing-title title-sm'})
        for prod in products:
            productie = prod.h3.text
            product_title.append(productie)
    for price_tag in product_info:
        prices = price_tag.span.text
        product_price.append(prices)

    users.append("1")
    administraties.append("1")

    for Product, price, user, admins in zip(product_title, product_price, users*len(product_title), administraties*len(product_title)):
        ondernemers_snelstart = dict(
            zip(headers, [Product, price, user, admins]))
        ondernemers_snelstart_info.append(ondernemers_snelstart)
    return ondernemers_snelstart_info


def snelstart_ondernemers_inhandel():

    url = 'https://www.snelstart.nl/inhandel'
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    soup = BeautifulSoup(html, 'html.parser')
    product_title = []
    product_price = []
    users = []
    administraties = []
    ondernemers_snelstart_inhandel_info = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    product_info = soup.find_all(
        'div', {'class': 'col-12 col-sm-12 col-md-4 col-lg-4 col-xl-4'})
    for div_tag in product_info:
        products = div_tag.find_all(
            'div', {'class': 'topmbox'})
        for prod in products:
            productie = prod.h3.text
            product_title.append(productie)
    for price_tag in product_info:
        p_tag = price_tag.find_all('p', {'class': 'price smallfont'})
        for each_p_tag in p_tag:
            pric = each_p_tag.text
        product_price.append(pric)

    users.append("1")
    administraties.append("1")
    for Product, price, user, admins in zip(product_title, product_price, users*len(product_title), administraties*len(product_title)):
        ondernemers_snelstart_inhandel = dict(
            zip(headers, [Product, price, user, admins]))
        ondernemers_snelstart_inhandel_info.append(
            ondernemers_snelstart_inhandel)
    return ondernemers_snelstart_inhandel_info




def snelstart_accountants():

    url = 'https://www.snelstart.nl/producten/accountants'
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    soup = BeautifulSoup(html, 'html.parser')
    product_title = []
    product_price = []
    users = []
    administraties = []
    snelstart_accountants_info = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    product_info = soup.find('div', {'class': 'col-md-5'})
    productie = product_info.h1.text
    product_title.append(productie)
    p_tag = soup.find('div', {'class': 'prijs hidden-xxs hidden-xs hidden-sm'})
    pric = p_tag.h3.text
    product_price.append(pric)

    users.append("2")
    administraties.append("1")

    for Product, price, user, admins in zip(product_title, product_price, users, administraties):
        accountants_snelstart = dict(
            zip(headers, [Product, price, user, admins]))
        snelstart_accountants_info.append(accountants_snelstart)
    return snelstart_accountants_info


def mergeAllData(uid=None):

    temp_dict = {}
    Ondernemers = snelstart_ondernemers() + snelstart_ondernemers_inhandel()
    temp_dict["Ondernemers"] = Ondernemers
    temp_dict["Accountants"] = snelstart_accountants()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = "Snelstart"
    return temp_dict