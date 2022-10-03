from wsgiref import headers
import requests
from bs4 import BeautifulSoup
from requests.models import Response
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from webscrape_backend.hashfile import HashMap

url = "https://appwiki.nl/afas-software"


endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "afas_productInfo"
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


def storeToAztable_Afas():
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


def afas():

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    pp = []
    title = []
    prices = []
    headers = ["ProductName", "Price"]
    for s in soup.find_all('div', {"class": "col-xs-12 col-sm-6"}):
        prtxt = s.text, s.next_sibling
        pp.extend(prtxt)
    b = pp[16:]
    data = [' '.join(ele.split()) for ele in b]
    while("" in data):
        data.remove("")
    for d in data:
        word = 'Proefperiode: Bekijk prijzen'
        splitword = d.split()
        newtext = [x for x in splitword if x not in word]
        title.append(newtext[0])
        prices.append(newtext[1]+" "+newtext[2])
    afas_info = []
    for product, price in zip(title, prices):
        product_info = dict(zip(headers, [product, price]))
        afas_info.append(product_info)

    return afas_info


def MergeAllData(uid=None):
    temp_dict = {}
    temp_dict["Afas"] = afas()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = uid
    return temp_dict

# storeToAztable_Afas()
