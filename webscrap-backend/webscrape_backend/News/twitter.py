"""
Twitter Scraper API
"""
import asyncio
from datetime import datetime
import tweepy
from azure.cosmos import exceptions, CosmosClient, PartitionKey

endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "TwitterNews"
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)


def storeToAzuretableTwitterNews():

    itemId = f"Item{0}"
    data = scrape_twitter(uid=itemId)
    container.upsert_item(data, partition_key=data["lastName"])


CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""

T_AUTH = tweepy.OAuth1UserHandler(
    CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
)

T_API = tweepy.API(T_AUTH)
T_URL = "https://twitter.com/"

USERNAMES = ("Exact_NL", "VismaSoftwareNL", "eboekhouden", "Wolters_Kluwer", "AFAS",
             "JorttNL", "SilvasoftOnline", "yukiboekhouden", "Informer_eu", "SnelStart",)


async def retrieve_tweets(user_names: tuple, tweet_count: int, retweets_flag: bool) -> dict:
    """retrieve_tweets: Fetch tweets belonging to each user in USERNAMES list

    Returns:
        dict: User and respective tweets
    """

    if not (
        (isinstance(user_names, tuple))
        and (isinstance(tweet_count, int))
        and (isinstance(retweets_flag, bool))
    ):
        raise TypeError("Invalid parameter types specified")

    user_tweet_info: dict = {}

    user_async_info = asyncio.gather(
        *[
            asyncio.create_task(
                fetch_user_tweets(
                    user_name=user,
                    user_info_dict=user_tweet_info,
                    user_tweets_count=tweet_count,
                    user_retweet_flag=retweets_flag
                )
            )
            for user in user_names
        ]
    )

    await asyncio.gather(user_async_info)

    return user_tweet_info


async def fetch_user_tweets(
    user_name: str, user_info_dict: dict, user_tweets_count: int, user_retweet_flag: bool
) -> dict:
    """update_user_tweet: Fetch user tweet info

    Args:
        user_name (str): User name

    Returns:
        dict:
    """

    user_tweets = T_API.user_timeline(
        screen_name=user_name,
        count=user_tweets_count,
        exclude_replies=True,
        include_rts=False,
        tweet_mode='extended'
    )

    user_info_dict[user_name] = []

    await update_user_tweet_info(tweets=user_tweets, user_tweets=user_info_dict[user_name])

    return user_info_dict[user_name]


async def update_user_tweet_info(tweets: list, user_tweets: list) -> None:
    """update_user_tweet_info: Update required tweet attributes of each tweet
    for respective user

    Args:
        tweets (list): Tweets of respective User
        user_tweet_container (dict): User tweet attribute container dictionary
    """

    date_format = "%Y-%m-%d %H:%M:%S"

    for each_tweet in tweets:
        user_tweet_container: dict = {}

        user_tweet_container["id"] = each_tweet.id_str
        user_tweet_container["date"] = datetime.strftime(
            each_tweet.created_at, date_format
        )
        user_tweet_container["text"] = each_tweet.full_text
        user_tweet_container["screen_name"] = each_tweet.user.screen_name
        user_tweet_container["likes"] = each_tweet.favorite_count
        user_tweet_container["source"] = "twitter.com"

        if "media" not in each_tweet.entities:
            user_tweet_container["media"] = ""
            user_tweet_container["url"] = \
                f"{T_URL}/{each_tweet.user.screen_name}/status/{each_tweet.id_str}"

        else:
            media_meta = [
                (each_media["media_url"], each_media["expanded_url"],)
                for each_media in each_tweet.entities["media"]
            ]
            user_tweet_container["media"], user_tweet_container["url"] = \
                list(zip(*media_meta))

        user_tweets.append(user_tweet_container)


def scrape_twitter(
    usernames: tuple = None, tweet_count: int = 60, fetch_retweets: bool = False, uid=None
):
    """user_tweets Fetch user tweets from given parameters

    Args:
        usernames (tuple, optional): tuple of usernames to fetch tweets. Defaults to None.
        tweet_count (int, optional): Number of tweets to fetch. Defaults to 60.
        fetch_retweets (bool, optional): Fetch the retweets. Defaults to False.

    Returns:
        dict: Tweet info for each user
    """

    if not usernames:
        usernames = USERNAMES

    async_loop = asyncio.get_event_loop()

    user_tweet_info = async_loop.run_until_complete(
        retrieve_tweets(
            user_names=usernames,
            tweet_count=tweet_count,
            retweets_flag=fetch_retweets
        )
    )

    async_loop.close()
    user_tweet_info["id"] = uid.split("Item")[1]
    user_tweet_info["lastName"] = uid

    return user_tweet_info
