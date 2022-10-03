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


def featuresinformer():

    itemId = f"Item{4}"
    data = merge_data(uid=itemId)
    container.upsert_item(data)


def informer_features():

    url = 'https://www.informer.nl/boekhoudprogramma/prijzen/'
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    soup = BeautifulSoup(html, 'html.parser')
    product_title = ["FeatureName"]
    product_info = soup.find('div', {'class': 'arp_allcolumnsdiv'})
    products = product_info.find_all(
        'div', {'class': 'bestPlanTitle package_title_first toggle_step_first'})
    for prod in products:
        productie = prod.text
        product_title.append(productie)

    ul_tag = soup.find_all(
        'ul', {"class": "arp_opt_options arppricingtablebodyoptions"})
    table_info = []
    ignore_ids = []
    for ul_id, each_ul_tag in enumerate(ul_tag):
        li_tag = each_ul_tag.find_all('li')
        col_info = []
        for li_id, each_li_tag in enumerate(li_tag):
            if li_id in ignore_ids:
                continue
            if each_li_tag.b:
                ignore_ids.append(li_id)
                continue
            if ul_id:
                p_text = each_li_tag.text.split(each_li_tag.p.text)[1].strip()
            else:
                p_text = each_li_tag.p.text.strip()
                p_id = p_text.split()[0]
                p_text = f"{p_id}{p_text.split(p_id)[1]}"

            if "✔" in p_text:
                p_text = True
            if not p_text:
                p_text = False
            col_info.append(p_text)
        table_info.append(col_info)

    product_feature_info = []
    for col_info in zip(*table_info):
        product_feature_info.append(
            dict(
                zip(
                    product_title,
                    col_info
                )
            )
        )
    return product_feature_info


def merge_data(uid=None):

    temp_dict = {}
    temp_dict["Informer_features"] = informer_features()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = "Informerfeatures"
    return temp_dict
