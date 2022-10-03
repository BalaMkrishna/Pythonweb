import datetime
import dateparser
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from azure.cosmos import exceptions, CosmosClient, PartitionKey


endpoint = "https://scrapdata.documents.azure.com:443/"
primaryKey = "VMxs6A0kuEzFTgG1CjNMTHXzFByVqHrAjLE5w4ucWyGBW87bPVZ6Jt5QPdrsk7ig6Zk5WmshAka47FtaXqJrOw=="
client = CosmosClient(endpoint, primaryKey)
DB_NAME = "scrapeddata"
container_name = "Articles"
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)


def storeToAzuretableAticleNews():

    itemId = f"Item{0}"
    data = mergeAllNews(uid=itemId)
    container.upsert_item(data, partition_key=data["lastName"])


def getAllNewsFromfd():
    url = "https://fd.nl/financiele-markten"
    resp = urlopen(url)
    newss = []
    dd = []
    newsLink = []
    fdsource = []
    soup = BeautifulSoup(resp, 'lxml')
    getDiv = soup.find('div', {'class': 'xs-12 l-8 gap-1'})
    newsHeading = getDiv.find_all('h1')
    for news in newsHeading:
        getNews = news.text
        newss.append(getNews)
        if len(newss) <= len(newsHeading):
            fdsource.append('fd.nl')
        else:
            pass
    getDate = getDiv.find_all('time')
    for d in getDate:
        date = d.text
        if ":" in date:
            convertToDate = datetime.today().strftime('%m-%d-%Y')
        elif "'" in date:
            date_month, year = date.split("'")
            if len(year) == 2:
                year = f'20{year}'
            else:
                year = f'{year}'
            complete_date = f'{date_month}{year}'
            convertToDate = datetime.strptime(complete_date, '%d %b %Y')
        else:
            todayYear = datetime.now().year
            date_parse = dateparser.parse(date)
            convertToDate = date_parse.strftime('%m-%d-'+str(todayYear))
        dd.append(convertToDate)
    nlink = getDiv.find_all('a', href=True)
    for l in nlink:
        link = 'https://fd.nl'+l['href']
        newsLink.append(link)

    return newss, dd, newsLink[:-1], fdsource


def getAllNewsFromSoftware():
    url = "https://www.softwarepakketten.nl/cmm/berichten/berichten_raadplegen.php?id=7&bronw=1"
    req = urlopen(url)
    title = []
    link = []
    dat = []
    soup = BeautifulSoup(req, 'lxml')
    softSource = []
    getContainer = soup.find_all('div', {'class': 'col-md-12'})
    for c in getContainer:
        ctl = c.find_all('a')
        for ct in ctl:
            ctitl = ct.text
            title.append(ctitl)
            if len(title) <= len(ctl):
                softSource.append('Softwarepakketten.nl')
            if ct.has_attr('href'):
                stripl = ct['href'].strip('../..')
                addht = 'https://www.softwarepakketten.nl/'+stripl
                link.append(addht)
        cd = c.find_all('font')
        for d in cd:
            dt = d.text
            stripd = dt.strip(' ()')
            convertDate = datetime.strptime(
                stripd, '%d-%m-%Y').strftime('%m-%d-%Y')
            dat.append(convertDate)

    return title[:-3], dat, link[:-3], softSource


def getAllNewsFromBoekhoudplaza():
    url = "https://www.boekhoudplaza.nl/cmm/berichten/berichten_raadplegen.php?id=7"
    req = requests.get(url)
    nt = []
    link = []
    nd = []
    BoekSource = []
    soup = BeautifulSoup(req.text, 'html.parser')
    getDiv = soup.find_all('div', {'class': 'col-md-12'})
    for dv in getDiv:
        dtxt = dv.find_all('a')
        for a in dtxt:
            atxt = a.text
            nt.append(atxt)
        if len(nt) <= len(dtxt):
            BoekSource.append('Boekhoudplaza.nl')
        ddate = dv.find_all('font')
        for d in ddate:
            ddatxt = d.text
            strtxt = str(ddatxt.strip(' ()'))
            convertDate = datetime.strptime(
                strtxt, '%d-%m-%Y').strftime('%m-%d-%Y')
            nd.append(convertDate)
        nlink = dv.find_all('a', href=True)
        for l in nlink:
            lk = l['href'].strip('../..')
            addlk = 'https://www.boekhoudplaza.nl/'+lk
            link.append(addlk)

    return nt[:-2], nd, link[:-2], BoekSource


def getAllNewsFromAccountancy():
    url = "https://www.accountancyvanmorgen.nl/categorie/accountancy/"
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    title = []
    date = []
    link = []
    AccSource = []
    getmain = soup.find('main', {'id': 'genesis-content'})
    getTitle = getmain.find_all('h2', {'class': 'entry-title'})
    for ti in getTitle:
        titl = ti.text
        title.append(titl)
        if len(ti) <= len(getTitle):
            AccSource.append('AccountancyVanmorgen.nl')
    getdate = getmain.find_all('div', {'class': 'entry-inner'})
    for d in getdate:
        dd = d.span.text
        date_string = dd.split(" ")
        final_date = f'{date_string[0]} {date_string[1][:3]} {date_string[2]}'
        date_data = dateparser.parse(final_date)
        convertToDate = date_data.strftime('%m-%d-%Y')
        date.append(convertToDate)
    nlink = getmain.find_all('a', href=True)
    for l in nlink:
        link.append(l['href'])

    return title, date, link[:-5], AccSource


def getAllNewsFromjorttpers():
    home_url = "https://www.jortt.nl"
    url = "https://www.jortt.nl/pers/"
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    title = []
    date = []
    link = []
    AccSource = []
    Persberichten = soup.find('div', {'class': 'article-row'})
    all_Persberichten = Persberichten.find_all("a", {"class": "article-block"})

    for each_post in all_Persberichten:
        if each_post:
            post_url = each_post.attrs.get("href")
            post_response = requests.get(f"{home_url}{post_url}")
            post_soup = BeautifulSoup(post_response.text, 'html.parser')
            article_info = post_soup.find("article")
            text_title = article_info.h1.text
            title.append(text_title)
            if len(each_post) <= len(all_Persberichten):
                AccSource.append('jortt.nl')
            date_info = post_soup.find("aside")
            dates = date_info.strong.text.split("-")[1].strip()
            date.append(dates)
            link.append(f"{home_url}{post_url}")
    return title, date, link, AccSource


def getAllNewsFromsilvasoftnieuws():

    url = "https://www.silvasoft.nl/nieuws/"
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    title = []
    date = []
    link = []
    AccSource = []
    getmain = soup.find('main', {'id': 'main'})
    getTitle = getmain.find_all('h4')
    for ti in getTitle:
        titl = ti.text
        title.append(titl)
        if len(ti) <= len(getTitle):
            AccSource.append('silvasoft.nl')

    getdate = getmain.find_all('p', {'class': 'meta'})
    for d in getdate:
        dd = d.find_all('span', {'class': 'updated'})
        for each_dd in dd:
            date_info = each_dd.text
            dt_obj = datetime.fromisoformat(date_info)
            dt_format = dt_obj.strftime("%d %B %Y")
            date.append(dt_format)
    nlink = getmain.find_all('a', href=True)
    for l in nlink:
        link.append(l['href'])

    return title, date, link[:-5], AccSource


def getAllNewsFromsnelstartproductnieuws():

    url = "https://www.snelstart.nl/productnieuws/all"
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    title = []
    date = []
    link = []
    AccSource = []
    ONDERWERPEN = soup.find('div', {'class': 'post-listing-simple'})
    all_ONDERWERPEN = ONDERWERPEN.find_all('a')
    for each_post in all_ONDERWERPEN:
        if each_post:
            post_url = each_post.attrs.get("href")
            post_response = requests.get(post_url)
            post_soup = BeautifulSoup(post_response.text, 'html.parser')
            article_info = post_soup.find(
                "span", {"id": "hs_cos_wrapper_name"})
            text_title = article_info.text
            title.append(text_title)
            if len(each_post) <= len(all_ONDERWERPEN):
                AccSource.append('snelstart-productnieuws')
            date_info = post_soup.find("div", {"style": "float: left;"})
            dates = " ".join(date_info.p.text.split("op:")
                             [1].strip().split(" ")[:3])
            date.append(dates)
            link.append(post_url)

    return title, date, link, AccSource


def getAllNewsFromsnelstartnieuwsberichten():

    url = "https://www.snelstart.nl/nieuwsberichten/all"
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    title = []
    date = []
    link = []
    AccSource = []
    ONDERWERPEN = soup.find('div', {'class': 'post-listing-simple'})
    all_ONDERWERPEN = ONDERWERPEN.find_all('a')
    for each_post in all_ONDERWERPEN:
        if each_post:
            post_url = each_post.attrs.get("href")
            post_response = requests.get(post_url)
            post_soup = BeautifulSoup(post_response.text, 'html.parser')
            article_info = post_soup.find(
                "span", {"id": "hs_cos_wrapper_name"})
            text_title = article_info.text
            title.append(text_title)
            if len(each_post) <= len(all_ONDERWERPEN):
                AccSource.append('snelstart-nieuwsberichten')
            date_info = post_soup.find("div", {"style": "float: left;"})
            dates = " ".join(date_info.p.text.split("op:")
                             [1].strip().split(" ")[:3])
            date.append(dates)
            link.append(post_url)

    return title, date, link, AccSource


def mergeAllNews(uid=None):

    temp_dict = {}
    fdtitle, fddate, fdlink, fdSource = getAllNewsFromfd()
    temp_dict["fd_nl"] = list(zip(fdtitle, fddate, fdlink, fdSource))
    sftitle, sfdate, sflink, SoftSource = getAllNewsFromSoftware()
    temp_dict["Softwarepakketten_nl"] = list(
        zip(sftitle, sfdate, sflink, SoftSource))
    bptitle, bpdate, bplink, boekSource = getAllNewsFromBoekhoudplaza()
    temp_dict["Boekhoudplaza_nl"] = list(
        zip(bptitle, bpdate, bplink, boekSource))
    actitle, acdate, aclink, accSource = getAllNewsFromAccountancy()
    temp_dict["AccountancyVanmorgen_nl"] = list(
        zip(actitle, acdate, aclink, accSource))
    jdtitle, jddate, jdlink, jdSource = getAllNewsFromjorttpers()
    temp_dict["jortt_nl"] = list(zip(jdtitle, jddate, jdlink, jdSource))
    sldtitle, slddate, sldlink, sldSource = getAllNewsFromsilvasoftnieuws()
    temp_dict["silvasoft_nl"] = list(
        zip(sldtitle, slddate, sldlink, sldSource))
    sbdtitle, sbddate, sbdlink, sbdSource = getAllNewsFromsnelstartnieuwsberichten()
    temp_dict["snelstart_nieuwsberichten"] = list(
        zip(sbdtitle, sbddate, sbdlink, sbdSource))
    spdtitle, spddate, spdlink, spdSource = getAllNewsFromsnelstartproductnieuws()
    temp_dict["snelstart_productnieuws"] = list(
        zip(spdtitle, spddate, spdlink, spdSource))

    keys = ["text", "date", "post_url", "source"]
    final_dict = {}
    for source, info in temp_dict.items():
        final_dict[source] = []
        for source_info in info:
            final_dict[source].append(
                dict(zip(keys, source_info))
            )

    final_dict["id"] = uid.split("Item")[1]
    final_dict["lastName"] = uid

    return final_dict
