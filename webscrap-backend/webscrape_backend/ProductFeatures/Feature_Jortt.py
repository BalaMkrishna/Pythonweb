import requests
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from webscrape_backend.hashfile import HashMap

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


def featuresjortt():

    itemId = f"Item{1}"
    data = MergeAllData(uid=itemId)
    container.upsert_item(data)


def featuresondernemer():

    url = 'https://www.jortt.nl/prijs/'
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    product_features = {}
    product_tags = ['price-jortt-plus',
                    'price-jortt-compleet', 'price-jortt-gratis']
    for prod_tag in product_tags:
        prod = soup.find('div', {'id': prod_tag})
        product = prod.h2.text
        feature_info = prod.find('ul', {'class': 'price-list'})
        product_features[product] = []
        featur = feature_info.find_all('li')
        for fts in featur:
            complete_text = fts.text
            feat = ""
            if fts.small:
                feat = fts.small.text.strip()
            if feat:
                header, *_ = complete_text.split(feat)
                category_info = {}
                category_info["category"] = header
                category_info["feature"] = " ".join(
                    [text for text in feat.replace("\n", " ").strip().split(" ") if text])

                product_features[product].append(category_info)

    return product_features


def MergeAllData(uid=None):

    temp_dict = {}
    temp_dict["Ondernemer"] = featuresondernemer()
    temp_dict["Boekhouder"] = {}
    temp_dict["Boekhouder"]["Boekhoudersportaal"] = temp_dict["Ondernemer"]["Plus"].copy()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = "Jorttfeatures"
    return temp_dict


