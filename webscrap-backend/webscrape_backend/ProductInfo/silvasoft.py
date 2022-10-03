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


mainUrl = " https://www.silvasoft.nl/"

all_sub_urls = {
    '/boekhoudprogramma/',
    '/factuurprogramma/',
    '/offerte-software/',
    '/sales-software/',
    '/product-en-voorraadbeheer',
    '/projectmanagement-software/',
    '/urenregistratie/',
    '/personeelsmanagement/',
    '/taken-agenda/',
    '/relatiebeheer/'
}


def storeToAztable_silvasoft():

    itemId = f"Item{2}"
    data = merge_data(uid=itemId)
    container.upsert_item(data)


def silvasoft(extra_path=""):

    actual_Url = mainUrl+extra_path
    req = requests.get(actual_Url)
    soup = BeautifulSoup(req.text, 'html.parser')
    product_info = soup.find_all(
        'div', {'class': 'fusion-column-wrapper fusion-flex-column-wrapper-legacy'})
    product_titile = product_info[0].h1.text
    price_info = soup.find_all('div', {'class': 'fusion-alignleft'})

    product_price = (price_info[1].span.text)

    return product_titile, product_price


def silvasoft_product_info():
    product_title, product_price, users, administraties = [], [], [], []
    for end_point in all_sub_urls:
        data = silvasoft(extra_path=end_point)
        product_title.append(data[0])
        product_price.append(data[1])
        users.append("1")
        administraties.append("1")

    product_title.append("Software voor boekhouders")
    product_price.append("Gratis")
    req = requests.get(
        "https://www.silvasoft.nl/administratiekantoor-accountant-software/")
    soup = BeautifulSoup(req.text, 'html.parser')
    user_info = soup.find('div', {'class': 'fusion-text fusion-text-16'})
    user_info_p = user_info.find_all("p")
    each_user = user_info_p[1].text
    users.append(each_user)
    admin_info = soup.find(
        'div', {'class': 'fusion-builder-row fusion-builder-row-inner fusion-row'})
    admin_info_p = admin_info.find_all("p")
    admins = []
    boekhouden_label = "Boekhouden"
    facturatie_label = "Facturatie"
    for each_admin in admin_info_p:
        admins_info = {}
        admin_data = each_admin.text
        administratie_id = admin_data.find("administratie")
        boekhouden_id = admin_data.find("Boekhouden")+len(boekhouden_label)
        facturatie_id = admin_data.find("Facturatie")
        admins_info["administratie"] = admin_data[:administratie_id].strip()
        admins_info["Boekhouden"] = admin_data[boekhouden_id +
                                               1:facturatie_id].strip()
        admins_info["Facturatie"] = admin_data[facturatie_id +
                                               len(facturatie_label)+1:].strip()
        admins.append(admins_info)
    administraties.append(admins)
    headers = ["ProductName", "Price", "Users", "Administraties"]
    silvasoft_info = []
    for ProductName, Price, Users, Adminis in zip(product_title, product_price, users, administraties):
        silvasoft_info.append(
            dict(zip(headers, [ProductName, Price, Users, Adminis])))
    return silvasoft_info


def merge_data(uid=None):
    temp_dict = {}
    temp_dict["silvasoft"] = silvasoft_product_info()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = "Silvasoft"
    return temp_dict
