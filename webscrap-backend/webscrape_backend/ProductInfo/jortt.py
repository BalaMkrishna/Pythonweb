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


def storeToAztable_jortt():
    itemId = f"Item{1}"
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


def ondernemer():

    url = 'https://www.jortt.nl/prijs/'
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    product_title = []
    product_price = []
    jortt_ondernemer_product_info = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    product_info = soup.find_all('div', {'class': 'prijs-block-header'})
    for prod in product_info:
        products = prod.h2.text
        product_title.append(products)
    price_info = soup.find_all('div', {'class': 'price-block-v3'})
    for price in price_info:
        all_prices = list(price.stripped_strings)
        if len(all_prices) == 5:
                prices = "".join(all_prices[2:4])
        elif len(all_prices) in [2, 3]:
            prices = "".join(all_prices[0:len(all_prices)])
        else:
            prices = all_prices[0]

        product_price.append(prices)

    url1 = 'https://www.jortt.nl/boekhouden/meerdere-gebruikers/'
    req = requests.get(url1)
    soup = BeautifulSoup(req.text, 'html.parser')
    users = []
    administraties = []

    user_info = soup.find_all('ul', {'class': 'checks'})
    users_in = user_info[0].find('li')
    user = users_in.strong.text
    users.append(user)

    admin_info = soup.find_all('ul', {'class': 'checks'})
    admins = admin_info[1].find_all('li')
    admin = admins[-1].text.replace("â\x82¬", "€").split("€")[1]
    administraties.append("€" + admin)

    for Product, price, user, admins in zip(product_title[::-1], product_price[::-1], users*len(product_title), administraties*len(product_title)):
        jortt_ondernemer = dict(zip(headers, [Product, price, user, admins]))
        jortt_ondernemer_product_info.append(jortt_ondernemer)

    return jortt_ondernemer_product_info


def boekhouder():

    url = 'https://www.jortt.nl/boekhouder/administratiekantoor/'
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    product_titile = []
    product_price = []
    jortt_boekhouder_product_info = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    product_info = soup.find_all('div', {'class': 'w-col w-col-6'})
    for prod in product_info:
        products = prod.find_all("p")
        info = products[3].text.split(".")[0]
        product = info.split("â\x82¬")
        product_titile.append("Boekhoudersportaal")
        product_price.append(r"€" + product[1].strip())
        break

    url1 = 'https://www.jortt.nl/boekhouden/meerdere-gebruikers/'
    req = requests.get(url1)
    soup = BeautifulSoup(req.text, 'html.parser')
    users = []
    administraties = []

    user_info = soup.find_all('ul', {'class': 'checks'})
    users_in = user_info[0].find('li')
    user = users_in.strong.text
    users.append(user)

    admin_info = soup.find_all('ul', {'class': 'checks'})
    admins = admin_info[1].find_all('li')
    admin = admins[-1].text.replace("â\x82¬", "€").split("€")[1]
    administraties.append("€" + admin)

    for Product, price, user, admins in zip(product_titile, product_price, users*len(product_titile), administraties*len(product_titile)):
        jortt_boekhouder = dict(zip(headers, [Product, price, user, admins]))
        jortt_boekhouder_product_info.append(jortt_boekhouder)

    return jortt_boekhouder_product_info


def MergeAllData(uid=None):

    temp_dict = {}
    temp_dict["Ondernemer"] = ondernemer()
    temp_dict["Boekhouder"] = boekhouder()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = "Jortt"
    return temp_dict
