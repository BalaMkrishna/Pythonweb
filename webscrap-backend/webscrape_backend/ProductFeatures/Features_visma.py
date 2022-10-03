from operator import index
from pyexpat import features
import requests
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import uuid
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from webscrape_backend.hashfile import HashMap

url = Request("https://nl.visma.com/eaccounting/voor-de-accountant/functionaliteit/",
              headers={'user-agent': 'Mozilla/5.0'})


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


def fetching():
    all_items = container.read_all_items()
    all_item_obj = []
    for each_item in all_items:
        unwanted_keys = ["id",
                         "lastName",
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


def featuresvisma():
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
    soup = BeautifulSoup(html, 'html.parser')
    main_dict = {}
    divs = ["ea-solo offer", "ea-standard offer", "ea-pro offer"]
    product_titles = []

    for each_div in divs:
        product = soup.find_all('div', {"class": each_div})
        product_titles.append(product[0].p.text.strip())

    ea_row_accordion = soup.find_all('div', {"class": "ea-row"})
    for each_row_accordion in ea_row_accordion:
        if hasattr(each_row_accordion, 'h4') and (each_row_accordion.h4 is not None):
            header = each_row_accordion.h4.text
            main_dict[header] = []
            continue
        ea_cells = each_row_accordion.find_all('div', {"class": "ea-cell"})
        if not ea_cells:
            continue
        row_data = [""]*4
        if ea_cells[0].p is None:
            if "Groothandelssoftware" in ea_cells[0].text.strip():
                break
        for cell_id, each_cell in enumerate(ea_cells):
            ea_cell_img = each_cell.find_all('img')
            ea_cell_p = each_cell.find_all('p')
            if ea_cell_p:
                row_data[cell_id] = ea_cell_p[0].text.strip()
            else:
                if cell_id == 0:
                    row_data[cell_id] = each_cell.text.strip()
                    continue
                row_data[cell_id] = bool(len(ea_cell_img))

        final_rw_data = dict(zip(["FeatureName"]+product_titles, row_data))
        main_dict[header].append(final_rw_data)
    for key in list(main_dict):
        if not main_dict[key]:
            del main_dict[key]

    return main_dict


def visma_eAccounting_Accountancy(url):
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    soup = BeautifulSoup(html, 'html.parser')
    ul_class = soup.find_all('div', {"class": "one-column-block__body"})
    features_dict = {}
    for ul in ul_class:
        uls = ul.find_all('li')
        for u in uls:
            feature = u.text.replace("\xa0", "")
            if (":" in feature) and (feature not in features_dict):
                temp_feature = feature.replace(":", "")
                features_dict[temp_feature] = []
                feature_key = temp_feature
                continue
            if feature not in features_dict[feature_key]:
                features_dict[feature_key].append(feature)
    visma_Accountancy_products = []
    product = soup.find_all('div', {"class": "one-column-block__body"})
    for p in product:
        all_products = p.find_all('strong')
        prod_text = [prod.text.strip() for prod in all_products]
        if not prod_text:
            continue
        visma_Accountancy_products.extend(prod_text[:-1])
    main_dict = {}
    for p_id, each_prod in enumerate(visma_Accountancy_products):
        product = each_prod[:each_prod.index("(")].strip()
        main_dict[product] = {}
        visma_Accountancy_products[p_id] = product

    for p_id, product in enumerate(visma_Accountancy_products):
        if p_id == 0:
            main_dict[product] = {
                key: value
                for key, value in features_dict.items()
                if key != "Visma Advisor"
            }
        else:
            main_dict[product] = features_dict

    return main_dict


def visma_accountview_solo(url):

    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    visma_accountview_solo = {}
    soup = BeautifulSoup(html, 'html.parser')
    features = soup.find_all('div', {"class": "teaser-block__content"})
    for feature in features:
        ft = feature.p.text
        break
    features_list = []
    for ul_class in features:
        li_class = ul_class.find_all('li')
        for li in li_class:
            fts = li.text
            features_list.append(fts)
    visma_accountview_solo[ft] = features_list[:6]
    product = soup.find_all('div', {"class": "teaser-block__headline"})
    all_products = product[0].h2.text.strip()
    main_dict = {}
    main_dict[all_products] = visma_accountview_solo

    return main_dict


def visma_accountview_team(url):
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    main_dict = {}
    soup = BeautifulSoup(html, 'html.parser')
    product = "Visma AccountView Team"
    feature_info = soup.find_all('div', {"class": "one-column-block__body"})
    features_info = []
    for features in feature_info:
        feature = features.find_all('ul')
        for ft in feature:
            featur = ft.find_all('li')
            for fts in featur:
                feat = fts.text.replace("\xa0", " ")
                features_info.append(feat)
    main_dict[product] = features_info
    return main_dict


def visma_accountview_business(url):
    response = Request(url, headers={'user-agent': 'Mozilla/5.0'})
    html = urlopen(response).read()
    main_dict = {}
    soup = BeautifulSoup(html, 'html.parser')
    product = "Visma AccountView Business"
    feature_info = soup.find_all('div', {"class": "one-column-block__body"})
    features_info = []
    for features in feature_info:
        feature = features.find_all('ul')
        for ft in feature:
            featur = ft.find_all('li')
            for fts in featur:
                feat = fts.text.replace("\xa0", " ")
                features_info.append(feat)

    main_dict[product] = features_info
    return main_dict


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
