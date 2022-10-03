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


def featuresyuki():

    itemId = f"Item{3}"
    data = MergeAllData(uid=itemId)
    container.upsert_item(data)


def ondernemers_yuki_features():

    url = 'https://www.yuki.nl/nl/ondernemers/pricing/1'
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    product_info = soup.find_all('ul', {'class': 'w-table__header w-cols'})
    product_title = []
    for prod in product_info:
        products = prod.find_all('li')
        for prod_info in products:
            product_title.append(prod_info.p.text)

    features = soup.find_all('div', {'class': 'w-table__item'})
    all_features = []
    for div_tag in features:
        li_tag = div_tag.find_all('li')
        features_list = []
        for each_li_tag in li_tag:
            icon = each_li_tag.find('div', {'class': 'check-icon'})
            p_tags = each_li_tag.find_all('p', {'style': 'font-size: 16px'})
            if p_tags:
                p_text = p_tags[0].text
            else:
                p_text = each_li_tag.text.strip()
            if icon:
                features_list.append(True)
            else:
                features_list.append(p_text)

        all_features.append(features_list)

    feature_info = []
    product_title[0] = "FeatureName"
    for feature in all_features:
        feature_info.append(dict(zip(product_title, feature)))
    return feature_info


def accountants_yuki_features():

    url = 'https://www.yuki.nl/nl/pricing'
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    product_info = soup.find_all('ul', {'class': 'w-table__header w-cols'})
    product_title = []
    products = product_info[0].find_all('li')
    for prod_info in products:
        productie = prod_info.p.text.split("\t")[0]
        product_title.append(productie)
    features = soup.find_all('div', {'class': 'w-table__item'})
    all_features = []
    for div_tag in features:
        li_tag = div_tag.find_all('li')
        features_list = []
        for each_li_tag in li_tag:
            icon = each_li_tag.find('div', {'class': 'check-icon'})
            p_tags = each_li_tag.find_all('p')
            if p_tags:
                p_text = p_tags[0].text
            else:
                p_text = each_li_tag.text.strip()
            if icon:
                features_list.append(True)
            else:
                p_text = p_text.strip()
                if "*" in p_text:
                    p_text = p_text.split("*")[0]
                features_list.append(p_text)

        all_features.append(features_list)

        if "Gepersonaliseerde URL" in features_list:
            break

    feature_info = []
    product_title[0] = "FeatureName"
    for feature in all_features:
        feature_info.append(dict(zip(product_title, feature)))
    return feature_info


def MergeAllData(uid=None):

    temp_dict = {}
    temp_dict["Ondernemers_features"] = ondernemers_yuki_features()
    temp_dict["Accountants_features"] = accountants_yuki_features()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = "Yukifeatures"
    return temp_dict
