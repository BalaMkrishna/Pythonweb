from os import sep
from pyexpat import features
import requests
from bs4 import BeautifulSoup
import re
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from webscrape_backend.hashfile import HashMap

url = "https://appwiki.nl/afas-software"


regex_pattern = re.compile(r".*?\>(.*)<.*")


endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "afas_productFeatures"
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)


def fetching():
    all_items = container.read_all_items()
    all_item_obj = []
    for each_item in all_items:
        unwanted_keys = ["id",
                         "_rid",
                         "_self",
                         "_etag",
                         "_attachments",
                         "_ts"]
        for keys in unwanted_keys:
            del each_item[keys]
        item_obj = HashMap(each_item)
        all_item_obj.append(item_obj)

    return all_item_obj


def featuresafas():

    itemId = f"Item{0}"
    data = MergeAllData(uid=itemId)
    data_copy = data.copy()
    del data_copy["id"]
    dobj = HashMap(data_copy)
    compare_documents(dobj, data)


def compare_documents(source_obj, original):
    target_list = fetching()
    total_count = len(target_list)
    if target_list:
        for each_target in target_list:
            if each_target.dict_value["lastName"] != original["lastName"]:
                continue
            if source_obj != each_target:
                original["id"] = str(total_count)
                container.create_item(original)
                total_count += 1
                break
    else:
        container.create_item(original)


def feature_afas():

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    row_labels = ["productName", "Features"]
    feature_info = []
    functionalites = soup.find_all('div', {"class": "accordion__body"})
    for u in functionalites:
        features = u.find_all('div', {"class": "col-xs-12 col-sm-6"})
        for ft in features:
            title = ft.h4.text
            functionalites_list = []
            col_rows = ft.find_all("div", {"class": "row"})
            for each_row in col_rows:
                functionalites_list.append(each_row.text.strip("\n"))

            product_info = dict(
                zip(row_labels, [title, functionalites_list]))
            feature_info.append(product_info)
        break

    return feature_info


def MergeAllData(uid=None):
    temp_dict = {}
    temp_dict["Afas"] = feature_afas()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = uid
    return temp_dict

# featuresafas()
