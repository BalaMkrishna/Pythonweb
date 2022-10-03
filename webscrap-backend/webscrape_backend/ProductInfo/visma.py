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


def storeToAztable_visma():
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


def visma_eAccounting(url):
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    visma_eAccounting = {}
    soup = BeautifulSoup(html, 'html.parser')
    visma_eaccount_products = []
    divs = ["ea-solo offer", "ea-standard offer", "ea-pro offer"]
    headers = ["ProductName", "Price"]
    for each_div in divs:
        pt = []
        product = soup.find_all('div', {"class": each_div})
        for p in product:
            title = p.text.strip()
            if "\n" in title:
                price_tag = title.split("\n")[0]
                title = price_tag[price_tag.find("€"):]
            pt.append(title)
        prod_dict = dict(zip(headers, pt))
        prod_dict["Users"] = None
        prod_dict["Administraties"] = None
        visma_eaccount_products.append(prod_dict)
    visma_eAccounting["Visma eAccounting"] = visma_eaccount_products
    return visma_eAccounting


def visma_eAccounting_Accountancy(url):
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    visma_Accountancy = {}
    soup = BeautifulSoup(html, 'html.parser')
    visma_Accountancy_products = []
    headers = ["ProductName", "Price"]
    product = soup.find_all('div', {"class": "one-column-block__body"})
    for p in product:
        all_products = p.find_all('strong')
        prod_text = [prod.text.strip() for prod in all_products]
        if not prod_text:
            continue
        visma_Accountancy_products.extend(prod_text[:-1])
        break
    product_prices = []
    for product_price in visma_Accountancy_products:
        price = re.findall(r"\((.*?)\)", product_price)[0]
        product = product_price.split(price)[0][:-1].strip()
        product_price = dict(zip(headers, [product, price]))
        product_price["Users"] = None
        product_price["Administraties"] = None
        product_prices.append(product_price)
    visma_Accountancy["visma Accountancy"] = product_prices
    return visma_Accountancy


def visma_accountview_solo(url):
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    visma_accountview_solo = {}
    soup = BeautifulSoup(html, 'html.parser')
    products = []
    price = []
    User_info = []
    Admins = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    product = soup.find_all('div', {"class": "teaser-block__headline"})

    all_products = product[0].h2.text.strip()
    products.append(all_products)
    all_prices = product[0].h3.text.strip()
    price.append(all_prices)
    user = soup.find_all('div', {"class": "teaser-block__content"})
    for u in user:
        p_class = u.find_all("p")
        users = p_class[1].text
        User_info.append(users)
        break
    for admin in user:
        ul_class = admin.find_all("li")
        admins = ul_class[0].text
        Admins.append(admins)
        break
    product_info = dict(
        zip(headers, [products[0], price[0], User_info[0], Admins[0]]))
    visma_accountview_solo["visma_accountview_solo"] = product_info
    return visma_accountview_solo


def visma_accountview_team(url):
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    visma_accountview_team = {}
    soup = BeautifulSoup(html, 'html.parser')
    products = "Visma AccountView Team"
    headers = ["ProductName", "Price", "Users", "Administraties"]
    prices = soup.find('div', {"class": "one-column-block__body"})
    p_class = prices.text
    price_info = re.findall(r"vanaf € [0-9]+,-", p_class)
    feature_info = soup.find_all('div', {"class": "one-column-block__body"})
    features_info = []
    for features in feature_info:
        feature = features.find_all('ul')
        for ft in feature:
            featur = ft.find_all('li')
            for fts in featur:
                feat = fts.text.replace("\xa0", " ")
                features_info.append(feat)
    product_info = dict(
        zip(headers, [products, price_info[0], features_info[0], features_info[1]]))
    visma_accountview_team["visma_accountview_team"] = product_info
    return visma_accountview_team


def visma_accountview_business(url):
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    visma_accountview_business = {}
    soup = BeautifulSoup(html, 'html.parser')
    products = "Visma AccountView Business"
    headers = ["ProductName", "Price", "Users", "Administraties"]
    prices = soup.find('div', {"class": "one-column-block__body"})
    p_class = prices.text
    price_info = re.findall(r"vanaf € [0-9]+,-", p_class)
    feature_info = soup.find_all('div', {"class": "one-column-block__body"})
    features_info = []
    for features in feature_info:
        feature = features.find_all('ul')
        for ft in feature:
            featur = ft.find_all('li')
            for fts in featur:
                feat = fts.text.replace("\xa0", " ")
                features_info.append(feat)

    product_info = dict(
        zip(headers, [products, price_info[0], features_info[0], features_info[1]]))
    visma_accountview_business["visma_accountview_business"] = product_info
    return visma_accountview_business


def MergeAllData(uid=None):
    temp_dict = {}
    url_func_map = {"eAccounting": visma_eAccounting,
                    "Accountancy": visma_eAccounting_Accountancy, "Accountview_solo": visma_accountview_solo,
                    "Accountview_team": visma_accountview_team, "Accountview_business": visma_accountview_business}
    url_map = {"eAccounting": "https://nl.visma.com/eaccounting/vergelijken/?click=txt_productblock_factureren",
               "Accountancy": "https://nl.visma.com/eaccounting/voor-de-accountant/visma-eaccounting-accountancy-meer-informatie/",
               "Accountview_solo": "https://nl.visma.com/accountview/accountview-solo/",
               "Accountview_team": "https://nl.visma.com/accountview/accountview-team/",
               "Accountview_business": "https://nl.visma.com/accountview/accountview-business/"}
    identifiers = ["eAccounting", "Accountancy",
                   "Accountview_solo", "Accountview_team", "Accountview_business"]

    for each_ident in identifiers:
        temp_dict[each_ident] = url_func_map[each_ident](
            url_map[each_ident])

    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = uid

    return temp_dict


