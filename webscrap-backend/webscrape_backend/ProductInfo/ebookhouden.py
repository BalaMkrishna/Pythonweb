from numpy import product
import requests
from bs4 import BeautifulSoup
from azure.cosmosdb.table.models import Entity
from webscrape_backend.hashfile import HashMap
from azure.cosmos import exceptions, CosmosClient, PartitionKey

mainUrl = "https://www.e-boekhouden.nl"


endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "eboekhouden_productInfo"
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


def storeToAztable_eboekhouden():

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


def ondernemers():
    subUrl = mainUrl+'/prijzen'
    response = requests.get(subUrl)
    soup = BeautifulSoup(response.content, 'html.parser')
    pt = []
    pi = []
    pr = []
    entrepreneurs = soup.find_all('div', {"class": "prijzen_doelgroep"})
    for e in entrepreneurs:
        a = e.find_all('h2')
        for ae in a:
            atxt = ae.text
            pt.append(atxt)
    products = soup.find_all(
        'div', {"class": "prijzen_container prijzen_transparant"})
    for p in products:
        pro = p.find_all('h2')
        for prod in pro:
            produc = prod.text
            pr.append(produc)

    users = soup.find_all('table', {"class": "prijzen_li_margin"})
    usersadmins = []
    for u in users:
        user = u.find_all('li')
        usersadmins = [u.text.replace("\t", "") for u in user]
        if usersadmins:
            break

    Invoicing = soup.find_all('div', {"class": "prijzen_info"})
    count = 0
    temp_c = []
    for I in Invoicing:
        c = I.find_all('h3')
        for ci in c:
            ctxt = '€ '+ci.text
            temp_c.append(ctxt)
        count += 1
        if c and (count != 0) and (count % 2 == 0):
            pi.append(temp_c)
            temp_c = []

    ondernemers_info = {}
    for c_id, category in enumerate(pt):
        ondernemers_info[category] = {}
        for p_id, product in enumerate(pr):
            ondernemers_info[category][product] = {}
            ondernemers_info[category][product]["ProductName"] = product
            ondernemers_info[category][product]["Price"] = pi[c_id][p_id]
            ondernemers_info[category][product]["Users"] = usersadmins[1]
            ondernemers_info[category][product]["Administraties"] = usersadmins[0]

    return ondernemers_info


def vereniging():
    subUrl = mainUrl+'/prijzen/verenigingen-stichtingen'
    response = requests.get(subUrl)
    soup = BeautifulSoup(response.content, 'html.parser')
    pt = []
    pi = []
    pr = []
    entrepreneurs = soup.find_all('div', {"class": "prijzen_doelgroep"})
    for e in entrepreneurs:
        a = e.find_all('h2')
        for ae in a:
            atxt = ae.text
            pt.append(atxt)
    products = soup.find_all(
        'div', {"class": "prijzen_container prijzen_transparant"})
    for p in products:
        pro = p.find_all('h2')
        for prod in pro:
            produc = prod.text
            pr.append(produc)
    users = soup.find_all('table', {"class": "prijzen_li_margin"})
    usersadmins = []
    for u in users:
        user = u.find_all('li')
        usersadmins = [u.text.replace("\t", "") for u in user]
        if usersadmins:
            break

    Invoicing = soup.find_all('div', {"class": "prijzen_info"})
    count = 0
    temp_c = []
    for I in Invoicing:
        c = I.find_all('h3')
        for ci in c:
            ctxt = '€ '+ci.text
            temp_c.append(ctxt)
        count += 1
        if c and (count != 0) and (count % 2 == 0):
            pi.append(temp_c)
            temp_c = []

    vereniging_info = {}
    for c_id, category in enumerate(pt):
        vereniging_info[category] = {}
        for p_id, product in enumerate(pr):
            vereniging_info[category][product] = {}
            vereniging_info[category][product]["ProductName"] = product
            vereniging_info[category][product]["Price"] = pi[c_id][p_id]
            vereniging_info[category][product]["Users"] = usersadmins[1]
            vereniging_info[category][product]["Administraties"] = usersadmins[0]

    return vereniging_info


def accountants():
    subUrl = mainUrl+'/prijzen/accountants-boekhouders'
    response = requests.get(subUrl)
    soup = BeautifulSoup(response.content, 'html.parser')
    pt = []
    administrations = []
    accountants = soup.find_all(
        'div', {"class": "prijzen_doelgroep prijzen_doelgroep_acc"})
    for category in accountants:
        ct = category.h2.text
        pt.append(ct)
    pt.append("Users")
    users = soup.find_all('table', {"class": "prijzen_li_margin"})
    usersadmins = []
    for u in users:
        user = u.find_all('li')
        usersadmins = [u.text.replace("\t", "") for u in user]
        if usersadmins:
            break
    prices = "Gratis"
    admin = soup.find_all(
        'div', {"class": "prijzen_info prijzen_pijl prijzen_acc"})
    for e in admin:
        a = e.find_all('td')
        temp = []
        for ae in a:
            atxt = ae.text
            temp.append(atxt)
        new_temp = []
        final_temp = []
        for t_id, val in enumerate(temp[2:]):
            if (t_id % 2 == 0) and (t_id != 0):
                final_temp.append(dict([new_temp]))
                new_temp = []
            new_temp.append(val)
        administrations.append(final_temp)

    administrations = dict(zip(pt, administrations))
    accountants_info = {}
    for key, value in administrations.items():
        accountants_info[key] = {}
        accountants_info[key]["ProductName"] = key
        accountants_info[key]["Price"] = "Gratis"
        accountants_info[key]["Users"] = usersadmins[1]
        accountants_info[key]["Administaties"] = value

    return accountants_info


def MergeAllData(uid=None):
    temp_dict = {}
    temp_dict["ondernemers"] = ondernemers()
    temp_dict["vereniging"] = vereniging()
    temp_dict["accountants"] = accountants()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = uid
    return temp_dict


# storeToAztable_eboekhouden()
