from itunes_app_scraper.scraper import AppStoreScraper
from pprint import pprint
from azure.cosmos import exceptions, CosmosClient, PartitionKey

endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "Reviews_Appstore"
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)


def appstorereviews():

    itemId = f"Item{0}"
    data = MergeAllData(uid=itemId)
    container.upsert_item(data)


def appstore_reviews():
    app_ids = ['627864429', '433619246', '1028433924', '1078225120', '1409821599',
               '1061083726', '1182704816', '1477029045', '994126299', '1189100644']
    alldata = []
    for app_id in app_ids:
        scraper = AppStoreScraper()
        result = scraper.get_app_details(app_id)
        app_store = {key: result[key] for key in result.keys() & {'averageUserRating', 'reviews',
                                                                  'trackViewUrl', 'trackName', 'description', 'userRatingCount'}}  # extract data from dict using key
        alldata.append(app_store)

    return alldata


def MergeAllData(uid=None):
    temp_dict = {}
    temp_dict["Appstore"] = appstore_reviews()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = uid
    return temp_dict



