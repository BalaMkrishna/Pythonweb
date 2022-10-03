from bs4 import BeautifulSoup
from urllib.request import urlopen
from azure.cosmos import exceptions, CosmosClient, PartitionKey


endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "Reviews_Trustpilot"
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)


def storeToAztable_reviewTrustpilot():

    itemId = f"Item{0}"
    data = MergeAllData(uid=itemId)
    container.upsert_item(data)


def getAllReviews():
    urls = ["https://www.trustpilot.com/review/www.exact.com", "https://www.trustpilot.com/review/nl.visma.com",
            "https://www.trustpilot.com/review/www.e-boekhouden.nl", "https://www.trustpilot.com/review/wolterskluwer.com",
            "https://www.trustpilot.com/review/afas.nl", "https://nl.trustpilot.com/review/jortt.nl", "https://nl.trustpilot.com/review/silvasoft.nl",
            "https://www.trustpilot.com/review/yuki.nl", "https://www.trustpilot.com/review/snelstart.nl"]
    reviews = []
    reviewCnt = []
    headers = ["WebsiteName", "Review", "Reviewcount"]
    websiteName = ["Exact", "Visma", "e-Boekhouden.nl",
                   "Wolters Kluwer", "Afas Software", "jortt.nl", "Silvasoft",
                   "Yuki", "SnelStart | maakt boekhouden makkelijk"]
    for url in urls:

        resp = urlopen(url)
        soup = BeautifulSoup(resp, 'lxml')
        getDiv = soup.find('div', {'class': 'styles_rating__NPyeH'})
        getRating = getDiv.find_all('p')
        for rate in getRating:
            review = rate.text.replace(",", ".")
            reviews.append(review)
        getRwCnt = soup.find('div', {'class': 'styles_summary__gEFdQ'})
        getrw = getRwCnt.find_all('span', {
                                'class': 'typography_typography__QgicV typography_bodysmall__irytL typography_color-gray-7__9Ut3K typography_weight-regular__TWEnf typography_fontstyle-normal__kHyN3 styles_text__W4hWi'})
        for rv in getrw:
            rwc = rv.text
            striprv = rwc.split(" • ")
            reviewCnt.append(striprv[0])

    review_info = []
    for website, review_data, reviewcount in zip(websiteName, reviews, reviewCnt):
        product_info = dict(zip(headers, [website, review_data, reviewcount]))
        review_info.append(product_info)

    return review_info


def MergeAllData(uid= None):

    temp_dict = {}
    temp_dict["Reviews"] = getAllReviews()
    temp_dict["id"] = uid.split("Item")[1]
    temp_dict["lastName"] = uid
    return temp_dict

