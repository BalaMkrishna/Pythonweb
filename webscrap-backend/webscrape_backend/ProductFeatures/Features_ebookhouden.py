from os import sep
import requests
from bs4 import BeautifulSoup
import re
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from webscrape_backend.hashfile import HashMap


mainUrl = "https://www.e-boekhouden.nl/functies/overzicht"


regex_pattern = re.compile(r".*?\>(.*)<.*")
endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "eboekhouden_productFeatures"
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


def featureseboekhouden():

    itemId = f"Item{0}"
    data = html_parser(uid=itemId)
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


def html_parser(uid=None):

    req = requests.get(mainUrl)
    soup = BeautifulSoup(req.text, 'html.parser')
    tr_dict = {}
    td_header_class = "bkVINK bkBorder bkHeader"
    tr_header_class = "bkBorder titelfunctielijst"
    required_td_classes = ["bkBorder",
                           "bkVINK bkBorder bkOdd", "bkVINK bkBorder"]
    all_checks = {}
    for r_id, row_tag in enumerate(soup.find_all('tr')):
        header = row_tag.find_all('td', {"class": tr_header_class})
        if header:
            header_text = header[0].text.strip()
            if header_text:
                tr_dict[header_text] = []
                all_checks[header_text] = []
                header_classes = row_tag.find_all(
                    'td', {"class": td_header_class})
                check_headers = [" ".join(re.findall(r"\w+", h_tag.text))
                                 for h_tag in header_classes]

        if header_text in tr_dict:
            for each_td in required_td_classes:
                td_class = row_tag.find_all('td', {"class": each_td})

                if td_class:
                    if each_td in ["bkBorder"]:
                        if header_text != td_class[0].text.strip():
                            tr_dict[header_text].append(
                                td_class[0].text.strip())
                    else:
                        td_contents = [False if each_td_class.contents[0] == '\xa0' else True
                                       for each_td_class in td_class]
                        temp_check_data = dict(zip(check_headers, td_contents))
                        check_keys = list(temp_check_data)
                        for key in check_keys:
                            if not key:
                                del temp_check_data[key]
                        all_checks[header_text].append(temp_check_data)

    final_dict = {}
    for header in list(all_checks.keys()):
        final_dict[header] = []
        for index, each_val in enumerate(tr_dict[header]):
            final_dict[header].append({"FeatureName": each_val})
            final_dict[header][index].update(all_checks[header][index])

    final_dict["id"] = uid.split("Item")[1]
    final_dict["lastName"] = uid

    return final_dict


# html_parser()
# featureseboekhouden()
