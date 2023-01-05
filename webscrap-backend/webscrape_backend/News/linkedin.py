"""
Scrape LinkedIn User account data
"""

import time
from functools import reduce
from typing import Any
from azure.cosmos import (
    CosmosClient,
    PartitionKey,
)
from linkedin_api import Linkedin

endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "LinkedinNews"
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)


USER_PUBLIC_NAMES_MAP = {
    "Visma": ("visma-eaccounting", "Visma eAccounting", "9187396",),
    "Exact": ("exact", "Exact", "5141",),
    "Eboekhouden": ("e-Boekhouden.nl", "e-Boekhouden.nl", "397919",),
    "WoltersKluwer": ("wolters-kluwer", "Wolters Kluwer", "2483",),
    "AFASSoftware": ("afas-software", "AFAS Software", "29766",),
    "Jortt": ("jortt", "Jortt - Ondernemer Gericht Boekhouden™.", "",),
    "Silvasoft": ("silvasoft", "Silvasoft", "",),
    "Yuki": ("visma-yuki", "Visma | yuki", "",),
    "Informer": ("informer-online-nederland", "Informer Online Nederland B.V.", ""),
    "SnelStart": ("snelstart", "SnelStart", "",),
}


VALUE_MAP = {
    "id": "id",
    "date": "value:com.linkedin.voyager.feed.render.UpdateV2:actor:subDescription:accessibilityText",
    "text": "value:com.linkedin.voyager.feed.render.UpdateV2:commentary:text:text",
    "media": "value:com.linkedin.voyager.feed.render.UpdateV2:content:com.linkedin.voyager.feed.render.ImageComponent:images",
    "url": "permalink",
    "likes": "value:com.linkedin.voyager.feed.render.UpdateV2:socialDetail:totalSocialActivityCounts:numLikes",
    "comments": "value:com.linkedin.voyager.feed.render.UpdateV2:socialDetail:totalSocialActivityCounts:numComments",
}


class LinkedInCompanyInfo:
    """
    Basic Attributes of LinkedIn companies:
        - Display Name
        - Public Name
        - User Name
        - Company Posts
    """

    def __init__(self) -> None:
        self.display_name: str = ""
        self.public_name: str = ""
        self.company_urn_id: str = ""
        self.company_label: str = ""
        self.company_posts: list = []
        self.company_filtered_posts: list = []


def update_company_posts(
    company_instance: LinkedInCompanyInfo, post_count: int, companies_list: list
) -> None:
    """update_company_posts

    Args:
        company_instance (LinkedInCompanyInfo): LinkedInCompanyInfo instance
    """
    linkedin_obj = Linkedin(
        username="userID",
        password="password",
    )
    company_instance.company_posts = linkedin_obj.get_company_updates(
        public_id=company_instance.public_name,
        urn_id=company_instance.company_urn_id,
        max_results=post_count
    )

    update_user_linkedin_info(company_instance=company_instance)
    companies_list.append(company_instance)


def update_user_linkedin_info(company_instance: LinkedInCompanyInfo) -> list:
    """update_user_linkedin_info: Update required linkedin attributes of each linkedin
    for respective user

    Args:
        linkedin (list): linkedin of respective User
        user_linkedin_container (dict): User linkedin attribute container dictionary
    """

    for each_post in company_instance.company_posts:

        user_post_info = {}

        for attr_label, attr_path in VALUE_MAP.items():
            if attr_label == "media":
                image_meta = depth_search(attr_path, each_post)
                attr_info = fetch_image_info(image_meta)
            elif attr_label == "id":
                post_id: str = depth_search(attr_path, each_post)
                attr_info = post_id.strip("activity:")
            else:
                attr_info = depth_search(attr_path, each_post)

            user_post_info[attr_label] = attr_info

        user_post_info['screen_name'] = company_instance.public_name
        user_post_info['source'] = "linkedin.com"

        company_instance.company_filtered_posts.append(user_post_info)


def depth_search(element_path: str, ref_dict: dict) -> Any:
    """find recursive dictionary lookup for a given path

    Args:
        element_path (str): Lookup path for respective key
        ref_dict (dict): Reference dictionary to lookup

    Returns:
        Any: Value assigned for respective key
    """

    try:
        search_info = reduce(
            lambda elem, key: elem[key], element_path.split(":"), ref_dict)

    except KeyError:
        search_info = ""

    return search_info


def fetch_image_info(image_meta_data: list) -> list:
    """fetch_image_info: Fetch media urls from given image meta data

    Args:
        image_meta_data (list): Image metadata from linkedin API

    Returns:
        list: List of media urls
    """

    _artifacts_path = "vectorImage:artifacts"
    _media_identifier = "fileIdentifyingUrlPathSegment"
    images_found: list = []

    for each_image_meta in image_meta_data:
        selected_artifact = each_image_meta["attributes"][0]
        root_url = selected_artifact["vectorImage"]["rootUrl"]
        image_artifacts = depth_search(_artifacts_path, selected_artifact)
        final_url = f'{root_url}{image_artifacts[-1][_media_identifier]}'
        images_found.append(final_url)

    return images_found


def scrape_linkedin(usernames: dict = None, max_posts: int = 50):
    """scrape_linkedin: Uses multiprocessing to fetch and update company posts

    Args:
        usernames (dict, optional): Company profile. Defaults to None.
        max_posts (int, optional): Number of posts to fetch. Defaults to 50.

    Yields:
        Iterator[dict]: Updated company instance with filtered posts
    """

    if not usernames:
        usernames = USER_PUBLIC_NAMES_MAP

    company_info = []

    for user_name, (public_name, display_name, urn_id) in usernames.items():

        company_obj = LinkedInCompanyInfo()
        company_obj.display_name = display_name
        company_obj.public_name = public_name
        company_obj.company_urn_id = urn_id
        company_obj.company_label = user_name

        update_company_posts(company_obj, max_posts, company_info)

    return company_info

def store_in_azure_linkedIn():
    """store_in_azure:
    """
    row_key = f"Item{0}"
    company_posts = list(scrape_linkedin())
    final_company_info = {}
    for each_comp_info in company_posts:
        final_company_info[each_comp_info.company_label] = each_comp_info.company_filtered_posts

    final_company_info["id"] = "0"
    final_company_info["lastName"] = row_key
    container.upsert_item(final_company_info)



