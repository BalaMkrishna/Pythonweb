from time import time
from itertools import chain
from facebook_scraper import get_posts
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from numpy import append
from webscrape_backend.hashfile import HashMap


endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "FacebookNews"
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)


def fetching():
    all_items = container.read_all_items()
    unwanted_keys = ["id",
                     "lastName",
                     "_rid",
                     "_self",
                     "_etag",
                     "_attachments",
                     "_ts"]

    for each_item in all_items:
        item_page_dict = {}
        item_page_post_ids = []
        for page, page_info in each_item.items():
            item_page_dict[page] = page_info
            if page in unwanted_keys:
                del item_page_dict[page]

        item_page_post_ids = extract_post_ids(item_page_dict)

        yield item_page_post_ids


def extract_post_ids(ref_dict):

    all_page_post_ids = {}

    for page, page_info in ref_dict.items():
        all_page_post_ids[page] = [
            info["post_id"]
            for info in page_info
            if "post_id" in info
        ]

    return all_page_post_ids


def storeToAztableFacebook():

    itemId = f"Item{0}"
    data = getAllPosts(uid=itemId)
    compare_documents(data)
    container.upsert_item(data, partition_key=data["lastName"])


def compare_documents(scrapeddata):
    new_page_post_ids = {}
    original_page_post_ids = extract_post_ids(scrapeddata)
    for each_item_dict in fetching():
        for page, db_post_ids in each_item_dict.items():
            if original_page_post_ids[page] > db_post_ids:
                new_page_post_ids[page] = list(
                    set(original_page_post_ids[page])-set(db_post_ids))
            else:
                new_page_post_ids[page] = list(
                    set(db_post_ids)-set(original_page_post_ids[page]))

    new_page_posts = {}
    for page, post_ids_list in new_page_post_ids.items():
        if page not in new_page_posts:
            new_page_posts[page] = []
        for post in scrapeddata[page]:
            if str(post["post_id"]) in post_ids_list:
                new_page_posts[page].append(post)
        if not new_page_posts[page]:
            del new_page_posts[page]
    return new_page_posts


def getAllPosts(uid=None):
    SCRAPED_COLUMNS = (
        'post_id', 'time', 'text', 'likes', 'comments', 'image', 'post_url', 'username'
    )
    all_posts = {}
    pages = ['ExactWorld', 'VismaSoftware', 'eBoekhouden',
             'TwinfieldAccounting', 'AFASSoftware', 'JorttNL',
             'SilvasoftOnline', 'YukiSoftware', 'SnelStart']
    sources = 'facebook.com'
    for each_page in pages:
        temp_list = []

        for post in get_posts(each_page, pages=5, extra_info=True):
            required_columns = {
                each_col: post[each_col]
                for each_col in SCRAPED_COLUMNS
            }
            required_columns['source'] = sources
            required_columns["time"] = str(required_columns["time"])
            temp_list.append(required_columns)
        # print(each_page, len(temp_list))
        all_posts[each_page] = temp_list
    all_posts["id"] = uid.split("Item")[1]
    all_posts["lastName"] = uid

    return all_posts
