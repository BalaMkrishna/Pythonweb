from wsgiref.headers import Headers
from bs4 import BeautifulSoup
from urllib.request import urlopen
import azure
from azure.cosmos import exceptions, CosmosClient, PartitionKey
import uuid


endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "Reviews_Appwiki"
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)


def storeToAztable_reviewAppwiki():

    itemId = f"Item{0}"
    data = MergeAllData(uid=itemId)
    container.upsert_item(data)


def getAllReviews():
    urls = ["https://appwiki.nl/exact-online", "https://appwiki.nl/visma-software",
            "https://appwiki.nl/e-boekhouden", "https://appwiki.nl/twinfield", "https://appwiki.nl/afas-software",
            "https://appwiki.nl/jortt", "https://appwiki.nl/silvasoft", "https://appwiki.nl/yuki",
            "https://appwiki.nl/informer", "https://appwiki.nl/snelstart-software"]
    reviews = []
    reviewCnt = []
    headers = ["WebsiteName", "Review", "Reviewcount"]
    websiteName = ["Exact", "Visma", "e-Boekhouden.nl",
                   "Wolters Kluwer", "Afas Software", "jortt",
                   "Silvasoft", "Yuki", "InformerOnline", "SnelStart"]
    for url in urls:
        resp = urlopen(url)
        soup = BeautifulSoup(resp, 'lxml')
        getDiv = soup.find(
            'div', {'class': 'progress progress--brandpage'})
        if getDiv:
            getRating = getDiv.find_all(
                'span', {'class': 'progress__percentage'})
            for rate in getRating:
                review = rate.text
                reviews.append(review)

            getRwCnt = soup.find('div', {'class': 'u-sticky'})
            getrw = getRwCnt.find_all('h3')
            for rv in getrw:
                rwc = rv.text
                striprv = rwc.split(" ")
                reviewCnt.append(striprv[0])
        else:
            reviews.append("0")
            reviewCnt.append("0")

    review_info = []
    for website, review_data, reviewcount in zip(websiteName, reviews, reviewCnt):
        product_info = dict(zip(headers, [website, review_data, reviewcount]))
        review_info.append(product_info)

    return review_info


def MergeAllData(uid=None):
    temp_dict = {}
    temp_dict["Reviews"] = getAllReviews()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = uid
    return temp_dict


# storeToAztable_reviewAppwiki()
