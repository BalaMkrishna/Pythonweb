from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import re
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


def featuressnelstart():

    itemId = f"Item{5}"
    data = mergeAllData(uid=itemId)
    container.upsert_item(data)


def features_snelstart_ondernemers():

    url = 'https://www.snelstart.nl/accountant/pakketten-voor-klanten'
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    soup = BeautifulSoup(html, 'html.parser')
    product_title = []
    tr_tag = soup.find_all('tr')
    for title in tr_tag[0].find_all('th'):
        if not title.text:
            product_title.append("FeatureName")
        else:
            product_title.append(title.text.strip())

    all_features = []
    for each_tr_tag in tr_tag[3:]:
        feature_info = each_tr_tag.text.strip().split("\n")
        feature_list = []
        for each_feature in feature_info:
            if not each_feature:
                feature_list.append(False)
            else:
                if '✔' in each_feature:
                    feature_list.append(True)
                else:
                    feature_list.append(each_feature)

        all_features.append(dict(zip(product_title, feature_list)))

    return all_features


def features_snelstart_ondernemers_inhandel():

    url = 'https://www.snelstart.nl/inhandel'
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    soup = BeautifulSoup(html, 'html.parser')
    product_features = {}
    product_info = soup.find_all(
        'div', {'class': 'col-12 col-sm-12 col-md-4 col-lg-4 col-xl-4'})
    for div_tag in product_info:
        products = div_tag.find(
            'div', {'class': 'topmbox'})
        productie = products.h3.text
        product_features[productie] = []
        ul_tag = div_tag.find('ul')
        li_tag = ul_tag.find_all('li')
        for each_li_tag in li_tag:
            product_features[productie].append(each_li_tag.text.strip())

    return product_features


def features_snelstart_accountants():

    url = 'https://www.snelstart.nl/producten/accountants'
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    soup = BeautifulSoup(html, 'html.parser')
    product_title = []
    tr_tag = soup.find_all('tr')
    for title in tr_tag[0].find_all('th'):
        if not title.text:
            product_title.append("FeatureName")
        else:
            product_title.append(title.text.strip())

    all_features = []
    for each_tr_tag in tr_tag[3:]:
        feature_info = each_tr_tag.text.strip().split("\n")
        feature_list = []
        for each_feature in feature_info:
            if not each_feature:
                feature_list.append(False)
            else:
                if '✔' in each_feature:
                    feature_list.append(True)
                else:
                    feature_list.append(each_feature)

        all_features.append(dict(zip(product_title, feature_list)))

    return all_features


def mergeAllData(uid=None):

    temp_dict = {}
    temp_dict["Ondernemers"] = features_snelstart_ondernemers()
    temp_dict["inHandel"] = features_snelstart_ondernemers_inhandel()
    temp_dict["Accountants"] = features_snelstart_accountants()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = "Snelstartfeatures"
    return temp_dict
