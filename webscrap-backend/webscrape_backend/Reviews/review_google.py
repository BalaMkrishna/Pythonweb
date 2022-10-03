from bs4 import BeautifulSoup
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
import azure
from azure.cosmos import exceptions, CosmosClient, PartitionKey
import uuid
import googlemaps
import json


endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "Reviews_Google"
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)


def storeToAztable_reviewGoogle():

    itemId = f"Item{0}"
    data = MergeAllData(uid=itemId)
    container.upsert_item(data)


def getAllReviews():
    gmaps = googlemaps.Client(key='AIzaSyC9XImM_mVohtREYI2XKAFRdvc1QuVr2MY')

    site_name = ['exact software netherlands', 'visma software bv amsterdam',
                 'ebookhouden.nl', 'twinfield wolters kluwer', 'afas software', 'Jortt BV', 'Silvasoft',
                 'Yuki', 'Informer Online Nederland BV', 'SnelStart Alkmaar']
    headers = ["WebsiteName", "Review", "Reviewcount"]
    site_reviews = []
    site_reviewcount = []
    for site in site_name:
        places_result = gmaps.places(site)
        place_id = places_result['results'][0]['place_id']

        place = gmaps.place(place_id=place_id)
        data = json.dumps(place)
        result = json.loads(data)

        rating = result["result"]["rating"]
        site_reviews.append(rating)
        reviewCount = result["result"]["user_ratings_total"]
        site_reviewcount.append(reviewCount)
    review_info = []
    for website, review_data, reviewcount in zip(site_name, site_reviews, site_reviewcount):
        product_info = dict(zip(headers, [website, review_data, reviewcount]))
        review_info.append(product_info)

    return review_info


def MergeAllData(uid=None):

    temp_dict = {}
    temp_dict["Reviews"] = getAllReviews()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = uid
    return temp_dict

# storeToAztable_reviewGoogle()
