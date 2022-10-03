import requests
from itertools import chain
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from webscrape_backend.hashfile import HashMap

endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "visma_productFeatures"
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)

mainUrl = " https://www.silvasoft.nl/"

all_sub_urls = (
    '/boekhoudprogramma/',
    '/factuurprogramma/',
    '/relatiebeheer/',
    '/offerte-software/',
    '/sales-software/',
    '/product-en-voorraadbeheer',
    '/projectmanagement-software/',
    '/urenregistratie/',
    '/personeelsmanagement/',
    '/taken-agenda/',
)


def featuressilvasoft():

    itemId = f"Item{2}"
    data = MergeAllData(uid=itemId)
    container.upsert_item(data)


def feature_silvasoft(extra_path=""):

    actual_Url = mainUrl+extra_path
    req = requests.get(actual_Url)
    soup = BeautifulSoup(req.text, 'html.parser')
    product_features = {}
    product_info = soup.find_all(
        'div', {'class': 'fusion-column-wrapper fusion-flex-column-wrapper-legacy'})
    product = product_info[0].h1.text
    div_tag = soup.find('div', {'class': 'accordian fusion-accordian'})
    product_features[product] = []
    feature = div_tag.find_all('span', {'class': 'fusion-toggle-heading'})
    for each_feature in feature:
        product_features[product].append(each_feature.text)
    return product_features


def silvasoft_features():

    all_product_features = []
    for end_point in all_sub_urls:
        data = feature_silvasoft(extra_path=end_point)
        all_product_features.append(data)
    extra_product = {}
    extra_product["Software voor boekhouders"] = []
    extra_product["Software voor boekhouders"].extend(
        list(chain(*[
            list(product_feature.values())[0]
            for product_feature in all_product_features[:3]
        ]))
    )
    all_product_features.append(extra_product)

    return all_product_features


def MergeAllData(uid=None):
    temp_dict = {}
    temp_dict["silvasoft_features"] = silvasoft_features()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = "Silvasoftfeatures"
    return temp_dict
