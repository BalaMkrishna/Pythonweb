from os import sep
from pyexpat import features
import requests
from bs4 import BeautifulSoup
import re
import uuid
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from webscrape_backend.hashfile import HashMap

mainUrl = "https://www.wolterskluwer.com/nl-nl/solutions/twinfield-accounting/"

regex_pattern = re.compile(r".*?\>(.*)<.*")


endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "twinfield_productFeatures"
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


def featurestwinfield():

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


def accountants():
    subUrl = mainUrl+'accountants/abonnementen-prijzen'
    response = requests.get(subUrl)
    soup = BeautifulSoup(response.content, 'html.parser')
    row_info = []
    row_labels = ["productName", "Features"]
    features = soup.find_all('div', {"class": "ow-content-container"})
    for u in features:
        headers = u.find_all('div', {"class": "ow-column-details"})
        h = u.h5.text.strip().split("€")[0].strip()
        col_rows = u.find_all("li")
        feature_list = []
        for ft in col_rows:
            if ft.text.strip():
                v = ft.text.strip()
                feature_list.append(v)
        product_info = dict(
            zip(row_labels, [h, feature_list]))

        row_info.append(product_info)

    return row_info


def bedrijven():
    subUrl = mainUrl+'bedrijven/abonnementen-prijzen'
    response = requests.get(subUrl)
    soup = BeautifulSoup(response.content, 'html.parser')
    row_labels = ["productName", "Features"]
    features = soup.find_all('div', {"class": "ow-content-container"})
    row_info = []
    for u in features:
        headers = u.find_all('div', {"class": "ow-column-details"})
        h = u.h5.text.strip().split("€")[0].strip()
        col_rows = u.find_all("li")
        feature_list = []
        for ft in col_rows:
            if ft.text.strip():
                v = ft.text.strip()
                feature_list.append(v)
        product_info = dict(
            zip(row_labels, [h, feature_list]))

        row_info.append(product_info)

    return row_info


def bestaandeklanten():
    subUrl = mainUrl+'bedrijven/overzicht-abonnementen'
    response = requests.get(subUrl)
    soup = BeautifulSoup(response.content, 'html.parser')
    product = soup.find_all('div', {"class": "component-content"})
    row_labels = ["ProductName", "Feature"]
    table_rows = []
    bestaandeklanten_feature_info = {}
    for each_div in product:
        h2s = each_div.find_all("h2", {"id": "abonnementen-bestaande-klanten"})
        if not h2s:
            continue
        table_rows = each_div.find_all("tr")
        if not table_rows:
            continue
        break
    if not table_rows:
        return bestaandeklanten_feature_info

    row_info = []
    for each_row in table_rows[1:]:
        row_data = each_row.find_all("td")
        if not row_data:
            continue

        product_info = dict(
            zip(
                row_labels,
                [
                    row_data[0].text.strip(),
                    row_data[1].text.strip().replace("\xa0", " "),
                    None,
                    None
                ]
            )
        )

        row_info.append(product_info)

    return row_info


def MergeAllData(uid=None):

    temp_dict = {}
    temp_dict["accountants"] = accountants()
    temp_dict["bedrijven"] = bedrijven()
    temp_dict["bestaandeklanten"] = bestaandeklanten()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = uid
    return temp_dict

# featurestwinfield()
