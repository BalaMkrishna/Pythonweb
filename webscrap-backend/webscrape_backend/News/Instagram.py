from instagramy import *
import re
import requests
from datetime import datetime
from azure.cosmos import exceptions, CosmosClient, PartitionKey

endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "InstagramNews"
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)


def storeToAzuretableInstagramNews():

    itemId = f"Item{0}"
    get_sessionID = get_instagram_SessionId()
    data = get_Instagram_Post_Details(get_sessionID, uid=itemId)
    container.upsert_item(data, partition_key=data["lastName"])


def get_instagram_SessionId():

    link = 'https://www.instagram.com/accounts/login/'
    login_url = 'https://www.instagram.com/accounts/login/ajax/'

    time = int(datetime.now().timestamp())

    payload = {
        'username': 'balamanij94@gmail.com',
        'enc_password': f'#PWD_INSTAGRAM_BROWSER:0:{time}:Bala@1994',
        'queryParams': {},
        'optIntoOneTap': 'false'
    }

    with requests.Session() as s:
        r = s.get(link)
        csrf = re.findall(r"csrf_token\":\"(.*?)\"", r.text)[0]
        r = s.post(login_url, data=payload, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.instagram.com/accounts/login/",
            "x-csrftoken": csrf
        })

        sessionId = s.cookies.values()[6]

        return sessionId


def get_Instagram_Post_Details(sessionID, uid=None):

    Insta_Username = ['exact','visma_software_nl', 'afassoftware', 'eboekhouden', 'twinfield_wolterskluwer',
                      'jortt_boekhouden', 'silvasoft_eu', 'vismayuki', 'informer.nl', 'snelstart.nl']
    sources = 'Instagram.com'
    user_posts = {}
    for each_username in Insta_Username:
        user = InstagramUser(each_username, sessionid=sessionID)
        post_details = user.posts
        req_field = ['comments', 'likes', 'post_url',
                     'display_url', 'taken_at_timestamp']
        all_fields = req_field + ['text']
        all_posts = []
        for each_post in post_details:

            all_attr_values = [getattr(each_post, each_field)
                               for each_field in req_field]
            time_index = req_field.index("taken_at_timestamp")
            all_attr_values[time_index] = datetime.strftime(
                all_attr_values[time_index], "%Y-%m-%d %H:%M:%S")
            post_url = all_attr_values[2]
            post_id = post_url.split('/')[-2]

            try:
                post_text = InstagramPost(post_id, from_cache=True)
                post_text_data = post_text.text

            except KeyError:
                post_text_data = ""

            all_attr_values.append(post_text_data)
            create_data_dict = dict(zip(all_fields, all_attr_values))
            create_data_dict["source"] = sources
            create_data_dict["Username"] = each_username
            all_posts.append(create_data_dict)
        user_posts[each_username] = all_posts
    user_posts["id"] = uid.split("Item")[1]
    user_posts["lastName"] = uid
    return user_posts
