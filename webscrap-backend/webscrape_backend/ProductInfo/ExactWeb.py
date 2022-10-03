import requests
from bs4 import BeautifulSoup
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
import azure
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from webscrape_backend.hashfile import HashMap
from itertools import starmap

mainUrl = "https://www.exact.com/nl/producten"

endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "exact_productInfo"
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


def storeToAztable_ExactWeb():

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


def Boekhouden():
    subUrl = mainUrl+'/boekhouden/features-en-prijzen#features'
    req = requests.get(subUrl)
    soup = BeautifulSoup(req.text, 'html.parser')
    pt = []
    a = []
    pp = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    table = soup.find_all('th', attrs={'class': 'text-center'})
    for row in table:
        title = row.h4.text
        price = row.span.text
        pt.append(title)
        pp.append(price)

    userAccInc = soup.find_all('td', {'class': 'features-values text-center'})
    for u in userAccInc:
        t = u.text
        a.append(t)
    ui = a[1:4]
    ai = a[4:7]
    Boekhouden_info = []
    for product, price, user, admins in zip(pt, pp, ui, ai):
        product_info = dict(zip(headers, [product, price, user, admins]))
        Boekhouden_info.append(product_info)

    return Boekhouden_info


def Finance():
    subUrl = mainUrl+'/finance/features-en-prijzen#features'
    req = requests.get(subUrl)
    soup = BeautifulSoup(req.text, 'html.parser')
    pt = []
    pp = []
    a = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    table = soup.find_all('th', attrs={'class': 'text-center'})

    for t in table:
        title = t.h4.text
        price = t.span.text
        pt.append(title)
        pp.append(price)

    adminInc = soup.find_all(
        'td', {'class': 'features-values text-center'})
    for u in adminInc:
        t = u.text
        a.append(t)

    ai = a[0:2]
    ui = a[2:4]

    Finance_info = []
    for product, price, user, admins in zip(pt, pp, ui, ai):
        product_info = dict(zip(headers, [product, price, user, admins]))
        Finance_info.append(product_info)

    return Finance_info


def HR():
    subUrl = mainUrl+'/hr#abonnementen'
    req = requests.get(subUrl)
    soup = BeautifulSoup(req.text, 'html.parser')
    pt = []
    pp = []
    headers = ["ProductName", "Price"]
    getDiv = soup.find_all('div', attrs={'class': 'pricing-top'})
    for div in getDiv:
        pro = div.h3.text
        pt.append(pro)
        price = div.find_all('span', {'class': 'price'})
        for p in price:
            pr = p.text.strip()
        pp.append(pr)
    hr_info = []
    for product, price in zip(pt, pp):
        product_info = dict(zip(headers, [product, price]))
        product_info["Users"] = None
        product_info["Administraties"] = None
        hr_info.append(product_info)
    return hr_info


def PurchasetoPay():
    subUrl = mainUrl+'/purchase-to-pay'
    req = requests.get(subUrl)
    soup = BeautifulSoup(req.text, 'html.parser')
    pt = []
    pp = []
    headers = ["ProductName", "Price"]
    getDiv = soup.find_all('div', attrs={'class': 'pricing-top'})
    for div in getDiv:
        pro = div.h3.text
        pt.append(pro)
        price = div.find_all('span', {'class': 'on-request'})
        for p in price:
            pr = p.text
        pp.append(pr)
    PurchasetoPay_info = []
    for product, price in zip(pt, pp):
        product_info = dict(zip(headers, [product, price]))
        product_info["Users"] = None
        product_info["Administraties"] = None
        PurchasetoPay_info.append(product_info)
    return PurchasetoPay_info


def Bouw():
    subUrl = mainUrl+'/bouw/features-en-prijzen#features'
    req = requests.get(subUrl)
    soup = BeautifulSoup(req.text, 'html.parser')
    pt = []
    pp = []
    a = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    table = soup.find_all('th', attrs={'class': 'text-center'})
    for row in table:
        title = row.h4.text
        pt.append(title)
        price = row.p.text
        pp.append(price)

    userAccInc = soup.find_all('td', {'class': 'feature-value text-center'})
    for u in userAccInc:
        t = u.text
        a.append(t)

    ui_1 = list(map(int, a[0:3]))
    ui_2 = list(map(int, a[3:6]))

    ui = map(str, list(starmap(lambda x, y: x+y, zip(ui_1, ui_2))))
    ai = a[6:9]
    Bouw_info = []
    for product, price, user, admins in zip(pt, pp, ui, ai):
        product_info = dict(zip(headers, [product, price, user, admins]))
        Bouw_info.append(product_info)

    return Bouw_info


def Productie():
    subUrl = mainUrl+'/productie/features-en-prijzen#features'
    req = requests.get(subUrl)
    soup = BeautifulSoup(req.text, 'html.parser')
    pt = []
    pp = []
    a = []
    productie_info = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    table = soup.find_all('th', attrs={'class': 'text-center'})
    for row in table:
        title = row.h4.text
        pt.append(title)
        if row.find('span'):
            price = row.span.text
            pp.append(price)
        elif row.find('p'):
            price = row.p.text
            pp.append(price)

    userAccInc = soup.find_all('td', {'class': 'feature-value text-center'})
    for u in userAccInc:
        t = u.text
        a.append(t)

    ai = a[0:4]
    ui = a[4:8]

    for product, price, user, admins in zip(pt, pp, ui, ai):
        product_info = dict(zip(headers, [product, price, user, admins]))
        productie_info.append(product_info)

    return productie_info


def Handel():
    subUrl = mainUrl+'/handel/features-en-prijzen#features'
    req = requests.get(subUrl)
    soup = BeautifulSoup(req.text, 'html.parser')
    a = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    table = soup.find_all('th', attrs={'class': 'text-center'})
    pt = []
    pp = []
    for row in table:
        title = row.h4.text
        pt.append(title)
        if row.find('span'):
            pr = row.span.text
            pp.append(pr)
        elif row.find('p'):
            price = row.p.text
            pp.append(price)

    userAccInc = soup.find_all('td', {'class': 'feature-value text-center'})
    for u in userAccInc:
        t = u.text
        a.append(t)

    ui = a[0:4]
    ai = a[4:8]

    Handel_info = []
    for product, price, user, admins in zip(pt, pp, ui, ai):
        product_info = dict(zip(headers, [product, price, user, admins]))
        Handel_info.append(product_info)

    return Handel_info


def Salaris():
    from itertools import starmap
    subUrl = mainUrl+'/salaris/features-en-prijzen#features'
    req = requests.get(subUrl)
    soup = BeautifulSoup(req.text, 'html.parser')
    headers = ["ProductName", "Price"]
    a = []
    table = soup.find_all('th', attrs={'class': 'text-center'})
    pt = []
    pp = []
    for row in table:
        title = row.h4.text
        titles = row.span.text
        t = title + titles
        pt.append(t)
    price = soup.find_all('span', {'class': 'prod-price'})
    for p in price:
        pr = p.text.strip()
        pp.append(pr)

    pp[0] = pp[0].strip("Demo aanvragenLees meer")
    pp[1] = " ".join(pp[1].split()[:-3])

    Salaris_info = []
    for product, price in zip(pt, pp):
        product_info = dict(zip(headers, [product, price]))
        product_info["Users"] = None
        product_info["Administraties"] = None
        Salaris_info.append(product_info)
    return Salaris_info


def UrenFactureng():
    subUrl = mainUrl+'/uren-en-facturen/features-en-prijzen#features'
    req = requests.get(subUrl)
    soup = BeautifulSoup(req.text, 'html.parser')
    pt = []
    pp = []
    a = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    table = soup.find_all('th', attrs={'class': 'text-center'})

    for row in table:
        title = row.h4.text
        pt.append(title)
        if row.find('span'):
            pr = row.span.text
            pp.append(pr)
        elif row.find('p'):
            price = row.p.text
            pp.append(price)

    userAccInc = soup.find_all('td', {'class': 'feature-value text-center'})
    for u in userAccInc:
        t = u.text
        a.append(t)

    ai = a[0:2]
    ui = a[2:4]

    UrenFactureng_info = []
    for product, price, user, admins in zip(pt, pp, ui, ai):
        product_info = dict(zip(headers, [product, price, user, admins]))
        UrenFactureng_info.append(product_info)

    return UrenFactureng_info


def ProjectManagement():
    subUrl = mainUrl+'/project-management/features-en-prijzen#features'
    req = requests.get(subUrl)
    soup = BeautifulSoup(req.text, 'html.parser')
    pt = []
    pp = []
    a = []
    headers = ["ProductName", "Price", "Users", "Administraties"]
    table = soup.find_all('th', attrs={'class': 'text-center'})

    for row in table:
        title = row.h4.text
        pt.append(title)
        if row.find('span'):
            pr = row.span.text
            pp.append(pr)
        elif row.find('p'):
            price = row.p.text
            pp.append(price)

    userAccInc = soup.find_all('td', {'class': 'feature-value text-center'})
    for u in userAccInc:
        t = u.text
        a.append(t)

    ui = a[0:4]
    ai = a[4:8]

    ProjectManagement_info = []
    for product, price, user, admins in zip(pt, pp, ui, ai):
        product_info = dict(zip(headers, [product, price, user, admins]))
        ProjectManagement_info.append(product_info)

    return ProjectManagement_info


def MergeAllData(uid=None):
    temp_dict = {}
    temp_dict["Boekhouden"] = Boekhouden()
    temp_dict["Finance"] = Finance()
    temp_dict["HR"] = HR()
    temp_dict["PurchasetoPay"] = PurchasetoPay()
    temp_dict["Bouw"] = Bouw()
    temp_dict["Productie"] = Productie()
    temp_dict["Handel"] = Handel()
    temp_dict["Salaris"] = Salaris()
    temp_dict["Uren & Factureng"] = UrenFactureng()
    temp_dict["ProjectManagement"] = ProjectManagement()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = uid
    return temp_dict

# storeToAztable_ExactWeb()
