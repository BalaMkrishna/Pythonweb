from hashlib import new
from os import sep
from numpy import delete
import requests
from bs4 import BeautifulSoup
from azure.cosmos import exceptions, CosmosClient, PartitionKey
import re
from webscrape_backend.hashfile import HashMap


mainUrl = " https://www.exact.com/nl/producten"

all_sub_urls = {
    '0': '/boekhouden/features-en-prijzen#features',
    '1': '/finance/features-en-prijzen#features',
    '2': '/bouw/features-en-prijzen#features',
    '3': '/productie/features-en-prijzen#features',
    '4': '/handel/features-en-prijzen#features',
    '5': '/salaris/features-en-prijzen#features',
    '6': '/uren-en-facturen/features-en-prijzen#features',
    '7': '/project-management/features-en-prijzen#features'
}

regex_pattern = re.compile(r".*?\>(.*)<.*")


endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "exact_productFeatures"
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
                         "product_name",
                         "_rid",
                         "_self",
                         "_etag",
                         "_attachments",
                         "_ts"]
        for keys in unwanted_keys:
            if keys in each_item:
                del each_item[keys]
        item_obj = HashMap(each_item)
        all_item_obj.append(item_obj)

    return all_item_obj


def featuresexact():
    for url_id, end_point in all_sub_urls.items():
        itemId = f"Item{url_id}"
        data = html_parser(extra_path=end_point, uid=itemId)
        data_copy = data.copy()
        del data_copy["id"]
        dobj = HashMap(data_copy)
        data["product_name"] = end_point.split("/")[1]
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


def html_parser(extra_path="", uid=None) -> dict:

    actual_Url = mainUrl+extra_path
    req = requests.get(actual_Url)
    soup = BeautifulSoup(req.text, 'html.parser')
    headers = ""
    tr_dict = {}
    headers_not_to_delete = ["Deployment opties"]
    td_dict = None
    td_flag = False
    product_title = []
    table = soup.find_all('th', attrs={'class': 'text-center'})

    product_title.insert(0, "FeatureName")
    for row in table:
        title = row.h4.text
        product_title.append(title)

    for s in soup.find_all('tr'):

        if (hasattr(s.td, "h4")) and (s.td.h4 != None):
            headers = re.findall(regex_pattern, str(s.td.h4))[0]

            if headers == "Top features":
                continue
            if "<span id=" in headers:
                trim_index = headers.index("<span id=")
                headers = headers.split(headers[trim_index:])[0]

            if " &amp; " in headers:
                headers = headers.replace(" &amp; ", " ")
            tr_dict[headers] = []
            td_dict = {}
            td_flag = True

        prtxt = s.text.lstrip().split("\n")[:-1]
        if not prtxt:
            continue
        if " i" in prtxt[0]:
            prtxt[0] = prtxt[0].strip(" i").strip()
        temp_list = []
        for td in s.find_all('td'):
            all_i_class_tags = td.find_all('i', {"class": "fa fa-check"})
            if not all_i_class_tags:
                temp_list.append("")
            else:
                temp_list.append(str(all_i_class_tags[0]))
        if any(temp_list):
            for ind, val in enumerate(temp_list):
                if '<i class="fa fa-check">' in val:
                    prtxt[ind] = True
                if (ind) and (not val):
                    prtxt[ind] = False
        new_td_dict = {}
        if td_dict is not None:
            td_dict[prtxt[0]] = prtxt[1:]
            prod_title_count = len(product_title)
            if len(prtxt) > prod_title_count:
                if "Deployment opties" in prtxt:
                    continue
                counter = 1
                while len(prtxt) > prod_title_count:
                    counter += 1
                    prtxt[0] = "\n".join(prtxt[:counter])
                    counter -= 1
                    prtxt.pop(1)

            if (headers in td_dict) and (headers not in headers_not_to_delete):
                del td_dict[headers]

            new_td_dict.update(dict(zip(product_title, prtxt)))
            new_td_dict["FeatureName"] = prtxt[0]

        if td_flag:
            td_flag = False
        else:
            if headers and new_td_dict:
                tr_dict[headers].append(new_td_dict)

    tr_dict["id"] = uid.split("Item")[1]
    tr_dict["lastName"] = uid

    return tr_dict

