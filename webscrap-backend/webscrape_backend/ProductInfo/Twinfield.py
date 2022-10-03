from math import prod
import requests
from bs4 import BeautifulSoup
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
import azure
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from webscrape_backend.hashfile import HashMap

mainUrl = "https://www.wolterskluwer.com/nl-nl/solutions/twinfield-accounting/"

endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "twinfield_productInfo"
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


def storeToAztable_Twinfield():

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
    pt = []
    pp = []
    pu = []
    accountants_product_info = []
    product = soup.find_all('h5', {"class": "ow-column-title"})
    for p in product:
        Pt = p.text.split(':')
        p = Pt[0].strip('\n')
        pt.append(p)

        pr = Pt[1].strip('\n')
        pp.append(pr)

    users = soup.find_all('p', {"class": "ow-feature-title"})
    for u in users:
        t = u.text
        pu.append(t)

    ui = pu[0:1]+pu[15:16]
    ai = pu[2:3]+pu[17:18]
    prod_labels = ["ProductName", "Price", "Users", "Administraties"]
    for Product, price, user, admins in zip(pt, pp, ui, ai):
        accountants = dict(zip(prod_labels, [Product, price, user, admins]))
        accountants_product_info.append(accountants)
    return accountants_product_info


def bedrijven():
    subUrl = mainUrl+'bedrijven/abonnementen-prijzen'
    response = requests.get(subUrl)
    soup = BeautifulSoup(response.content, 'html.parser')
    pt = []
    pp = []
    pu = []
    bedrijven_product_info = []
    product = soup.find_all('h5', {"class": "ow-column-title"})
    for p in product:
        Pt = p.text.split('€')
        p = Pt[0].strip('\n')
        pt.append(p)
        pr = '€'+Pt[1].strip('\n')
        pp.append(pr)

    users = soup.find_all('p', {"class": "ow-feature-title"})
    for u in users:
        t = u.text
        pu.append(t)

    ui = pu[0:1]+pu[17:18]+pu[34:35]
    ai = pu[1:2]+pu[18:19]+pu[35:36]
    prod_labels = ["ProductName", "Price", "Users", "Administraties"]
    for Product, price, user, admins in zip(pt, pp, ui, ai):
        bedrijven = dict(zip(prod_labels, [Product, price, user, admins]))
        bedrijven_product_info.append(bedrijven)
    return bedrijven_product_info


def bestaandeklanten():
    subUrl = mainUrl+'bedrijven/overzicht-abonnementen'
    response = requests.get(subUrl)
    soup = BeautifulSoup(response.content, 'html.parser')
    product = soup.find_all('div', {"class": "component-content"})
    row_labels = ["ProductName", "Price", "Users", "Administraties"]
    table_rows = []
    bestaandeklanten_info = {}
    for each_div in product:
        h2s = each_div.find_all("h2", {"id": "abonnementen-bestaande-klanten"})
        if not h2s:
            continue
        table_rows = each_div.find_all("tr")
        if not table_rows:
            continue
        break
    if not table_rows:
        return bestaandeklanten_info

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
                    row_data[2].text.strip().replace("\xa0", " "),
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


# storeToAztable_Twinfield()
