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


def storeToAztable_informer():

    itemId = f"Item{4}"
    data = merge_data(uid=itemId)
    container.upsert_item(data)


def informer():

    url = 'https://www.informer.nl/boekhoudprogramma/prijzen/'
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    soup = BeautifulSoup(html, 'html.parser')
    product_title = []
    product_price = []
    users = []
    administraties = []
    product_info = soup.find('div', {'class': 'arp_allcolumnsdiv'})
    products = product_info.find_all(
        'div', {'class': 'bestPlanTitle package_title_first toggle_step_first'})
    for prod in products:
        productie = prod.text
        product_title.append(productie)
    prices = product_info.find_all(
        'span', {'class': 'price_text_first_step toggle_step_first'})
    for price in prices:
        pric = price.text
        product_price.append(pric)
    div_ul_tag = product_info.find_all('ul')
    div_li_tag = div_ul_tag[0].find_all('li')

    admin_index = [
        t_id
        for t_id, tag in enumerate(div_li_tag)
        if tag.p.text == "Administraties"
    ][0]
    user_index = [t_id
                  for t_id, tag in enumerate(div_li_tag)
                  if tag.p.text == "Gebruikers**"
                  ][0]

    for each_ul in div_ul_tag[2:]:
        li_tag = each_ul.find_all('li')
        admin = li_tag[admin_index].span.text.strip(
            div_li_tag[admin_index].p.text)
        administraties.append(admin)
        user = li_tag[user_index].span.text.strip(
            div_li_tag[user_index].p.text)
        users.append(user)
    administraties.insert(0, "1")
    users.insert(0, "1+1")

    headers = ["ProductName", "Price", "Users", "Administraties"]
    informer_info = []
    for ProductName, Price, Users, Adminis in zip(product_title, product_price, users, administraties):
        informer_info.append(
            dict(zip(headers, [ProductName, Price, Users, Adminis])))

    return informer_info


def merge_data(uid=None):

    temp_dict = {}
    temp_dict["Informer"] = informer()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = "Informer"
    return temp_dict
