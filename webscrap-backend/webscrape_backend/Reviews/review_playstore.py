from google_play_scraper import app
from azure.cosmos import exceptions, CosmosClient, PartitionKey

endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "Reviews_Playstore"
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)


def playstorereviews():

    itemId = f"Item{0}"
    data = MergeAllData(uid=itemId)
    container.upsert_item(data)


def playstore_reviews():

    app_ids = ['com.visma.ruby', 'com.exact', 'nl.afas.pocket2',
               'eboekhouden.nl', 'com.wolterskluwer.samenwerkenklant', 'com.jorttmobile', 'com.mijn.silvasoftglobal',
               'com.yuki.assistant', 'nl.Informer.app', 'nl.snelstart.web']
    alldata = []
    for app_id in app_ids:
        result = app(app_id,
                     lang='en',
                     country='NL')
        app_store = {key: result[key] for key in result.keys() & {'title', 'description', 'summary',
                                                                  'score', 'ratings', 'reviews', 'icon', 'url'}}  # extract data from dict using key
        alldata.append(app_store)

    return alldata

def MergeAllData(uid=None):
    temp_dict = {}
    temp_dict["Playstore"] = playstore_reviews()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = uid
    return temp_dict

# playstorereviews()
