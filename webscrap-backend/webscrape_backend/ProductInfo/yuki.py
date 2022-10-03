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

def storeToAztable_yuki():

    itemId = f"Item{3}"
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

def ondernemers_yuki():

    url = 'https://www.yuki.nl/nl/ondernemers/pricing/1'
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    headers = ["ProductName", "Price", "Users", "Administraties"]
    product_titile = []
    product_price = []
    ondernemers_yuki_product_info = []
    product_info = soup.find_all('ul', {'class': 'w-table__header w-cols'})
    for prod in product_info:
        products = prod.find_all('li')
        for prod_info in products:
            productie = prod_info.p.text
            product_titile.append(productie)
        prices = prod.find_all('li')
        for price in prices:
            price_info = price.find_all('span')
            if price_info:
                pric = price_info[0].text
                product_price.append(pric)

    ul_tag = soup.find_all('ul', {'class': 'w-table__item-data w-cols'})
    users = []
    admins = []
    for ul_id, each_ul_tag in enumerate(ul_tag):
        if ul_id in (0, 2,):
            li_tag = each_ul_tag.find_all('li')
            for each_li in li_tag[1:4]:
                if ul_id == 0:
                    users.append(each_li.p.text)
                else:
                    admins.append(each_li.p.text + "per extra administratie")

    for Product, price, user, admins in zip(product_titile[1:], product_price, users, admins):
        yuki_ondernemers = dict(zip(headers, [Product, price, user, admins]))
        ondernemers_yuki_product_info.append(yuki_ondernemers)

    return ondernemers_yuki_product_info


def accountants_yuki():

    url = 'https://www.yuki.nl/nl/pricing'
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    product_title = []
    product_price = []
    users = []
    admins = []
    accountants_yuki_product_info = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    product_info = soup.find_all('ul', {'class': 'w-table__header w-cols'})
    products = product_info[0].find_all('li')
    for prod_info in products[1:]:
        productie = prod_info.p.text.split("\t")[0]
        product_title.append(productie)
    prices = product_info[0].find_all('li')
    for price in prices:
        price_info = price.find_all('span')
        if not price_info:
            continue
        pric = price_info[2].text
        actual_price = ""
        for text in pric:
            if text.isdigit():
                actual_price += text
        if actual_price:
            final_price = str(int(actual_price) + 9)
            pric = pric.replace(actual_price, final_price)
        product_price.append(pric)

    users.append("1")
    admins.append("1 (9,- voor extra administratie)")

    for Product, price, user, admins in zip(product_title, product_price, users*len(product_title), admins*len(product_title)):
        yuki_accountants = dict(zip(headers, [Product, price, user, admins]))
        accountants_yuki_product_info.append(yuki_accountants)

    return accountants_yuki_product_info


def MergeAllData(uid=None):

    temp_dict = {}
    temp_dict["Ondernemers"] = ondernemers_yuki()
    temp_dict["Accountants"] = accountants_yuki()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = "Yuki"
    return temp_dict