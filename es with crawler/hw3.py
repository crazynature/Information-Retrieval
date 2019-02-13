from scrapy.utils.url import canonicalize_url, url_is_from_any_domain, url_has_any_extension
from bs4 import BeautifulSoup
import requests
import redis
import queue
import urllib.robotparser
from elasticsearch import Elasticsearch
import requests
import sys
import io
from lxml import html
import urllib
import time


class hw3:
    def __init__(self):
        self.seedUrl = "http://en.wikipedia.org"
        self.rp = urllib.robotparser.RobotFileParser()
        self.rp.set_url(self.seedUrl+"/robots.txt")
        self.rp.read()
    def crawler(self):
        # es = Elasticsearch(['35.227.82.63:9200'])
        es = Elasticsearch()
        f=open("url.txt",'w')
        if es.ping():
            r = redis.StrictRedis(host='35.227.82.63', port=6379)
            seen = []
            url_queue = queue.Queue()
            url_queue.put('http://en.wikipedia.org/wiki/Catholic_Church')
            url_queue.put('http://en.wikipedia.org/wiki/Christianity')
            url_queue.put('http://en.wikipedia.org/wiki/College_of_Cardinals')
            url_queue.put('https://en.wikipedia.org/wiki/Hierarchy_of_the_Catholic_Church')
            url_queue.put('https://en.wikipedia.org/wiki/Papal_legate')
            url_queue.put('http://www.bible.ca/catholic-church-hierarchy-organization.htm')
            url_queue.put('https://www.britannica.com/topic/Roman-Catholicism/Structure-of-the-church')
            seen.append('http://en.wikipedia.org/wiki/Christianity')
            seen.append('http://en.wikipedia.org/wiki/College_of_Cardinals')
            seen.append('https://en.wikipedia.org/wiki/Hierarchy_of_the_Catholic_Church')
            seen.append('https://en.wikipedia.org/wiki/Papal_legate')
            seen.append('http://www.bible.ca/catholic-church-hierarchy-organization.htm')
            seen.append('https://www.britannica.com/topic/Roman-Catholicism/Structure-of-the-church')
            count = 0
            tempurl=[]
            while not url_queue.empty() and count <= 1000:
                url = url_queue.get()
                seedUrl = self.getSeedUrl(url)
                if not self.robotCheck(url,seedUrl):
                    continue
                response = requests.get(url)
                Httptype = (response.headers['Content-Type'])
                if "text/html" in Httptype:
                    count += 1
                    headers = dict(response.headers)
                    soup = BeautifulSoup(response.content, 'html.parser')
                    seen.append(url)
                    r.set(url, "visited")
                    # data = []
                    out_links = []
                    print("init passed")
                    # for p in soup.find_all("p"):
                    #     data.append(self.getCleanText(str(p)))
                    data = self.getCleanText(str(soup.get_text))
                    rawHtml = str(soup.prettify())
                    for a in soup.find_all('a'):
                        temp = a.attrs
                        if 'title' in temp.keys():
                            score = self.checkKeywords(temp['title'])
                            # if self.checkKeywords(temp['title']):
                            if score > 0:
                                link = temp['href']
                                link = self.canonicalizeUrl(link, seedUrl)
                                if link not in out_links and link is not url:
                                    out_links.append(link)
                                # if r.get(link) == None and count<=1000:
                                if link not in seen  and count <= 1000:
                                    # url_queue.put(link)
                                    tempurl.append([link,score])
                                    seen.append(link)
                    print("outlink passed")
                    es.create("ap_dataset", "document", count,
                              body={"url": url, "inlinks": url, "outlinks": out_links, "text": data, "raw": rawHtml,
                                    "headers": (headers)})
                    print(url + " crawled "+str(count))
                    f.writelines(url + " crawled "+str(count)+'\n')
                    if url_queue.empty():
                        tempurl = self.sortUrl(tempurl)
                        for x in tempurl:
                            url_queue.put(x[0])
                        tempurl = []
        f.close()

    def sortUrl(self, data):
        sortedData = sorted(data, key=lambda s: s[1], reverse=True)
        return sortedData


    def canonicalizeUrl(self, url, seedUrl):
        if not url.startswith("http"):
            url = seedUrl + url
        temp = url.split("/")
        result = temp[0] + "//" + temp[2]
        if len(temp) > 2:
            for num in range(3, len(temp)):
                if len(temp[num]) > 0:
                    result = result + "/" + temp[num]
        result = canonicalize_url(result)
        return result

    def getSeedUrl(self, url):
        temp = url.split("/")
        result = temp[0] + "//" + temp[2]
        return result

    def getCleanText(self, content):
        temp = content.split("<")
        result = ""
        for term in temp:
            if ">" in term:
                term = term.split(">")
                for x in term:
                    if "href=" not in x and "/a" not in x and "/p" not in x and "id=" not in x and "/sup" not in x:
                        if x != "p":
                            result = result + x
            else:
                result = result + term
        return result

    def checkKeywords(self, target):
        keywords = ["bible", "church", "catholic", "roman", "christianity", "bishop", "religious",
                    "civilisation", "rome", "holy see", "vatican city", "italy", "nicene creed", "jesus",
                    "jesus christ", "christ", "saint peter", "christian", "churches", "enclosed religious orders",
                    "ecumenical councils", "cardinals", "antipope", "schism", "abrahamic", "religion", "messiah",
                    "christians", "savior", "god", "old testament", "judaism", "eternal life", "heaven", "angel",
                    "father","holy", "bibical", "cardinals", "archbishops", "priests", "catholic"]
        count = 0
        target = target.split(" ")
        for term in target:
            if term.lower() in keywords:
                count += 1
        return count

    def robotCheck(self,seedurl,target):
        if seedurl is not self.seedUrl:
            self.seedUrl = seedurl
            self.rp = urllib.robotparser.RobotFileParser()
            self.rp.set_url(seedurl+"/robots.txt")
            self.rp.read()
        else:
            time.sleep(1)
        # rp = urllib.robotparser.RobotFileParser()
        # rp.set_url(seedurl+"/robots.txt")
        # rp.read()
        return self.rp.can_fetch("*", target)

if __name__ == "__main__":
    test = hw3()
    # test.crawler()
    # url='https://movie.douban.com/' #需要爬数据的网址
    # page=requests.Session().get(url)
    # tree=html.fromstring(page.text)
    # result=tree.xpath('//td[@class="title"]//a/text()') #获取需要的数据
    # print(result)
    # sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='gb18030')
    # rp = urllib.robotparser.RobotFileParser()
    # rp.set_url("http://en.wikipedia.org/robots.txt")
    # rp.read()
    # print(rp.can_fetch("*", "http://en.wikipedia.org/wiki/Catholic_Church"))
    url = 'http://en.wikipedia.org/wiki/Catholic_Church'
    response = requests.get(url)
    # print(response.headers['Content-Type'])
    # print(response)
    soup = BeautifulSoup(response.content, 'html.parser')
    # temp = str(hw3.getCleanText(str(soup.find_all('p')[3])))
    # if "Main page" in temp:
    #     print(1)
    # print(soup.find_all('a')[50])
    # print(soup.find_all('a')[50].name)
    # print(soup.find_all('a')[50]["text"])
    # print(len(soup.find_all('a')))
    print(soup.find_all('p')[3])
    # print(test.checkKeywords(soup.find_all('p')[3]))
    print(test.getCleanText(str(soup.find_all('p')[3])))

    # print(soup.find_all('table')[1])
    # print(soup.text)
    # f = open("result.txt", "w")
    #
    # f.close()
    # es = Elasticsearch(['35.227.82.63:9200'])

    # if es.ping():
    #     print('ES connection okay')
    r = redis.StrictRedis(host='35.227.82.63', port=6379)

    # r.set('http://www.google.com', 'visited')
    if r.get('http://www.jdj') == None:
        print(1)
    # print(r.get('http://www.something.else.com'))
    #
    # print(test.canonicalizeUrl('http://www.example.com//a.html#anything', "http://www.example.com"))
    # print(test.canonicalizeUrl('/c/html', "http://www.example.com"))
    # if "//" in "http:/www.example.com/a.html":
    #     print(1)
    #     print("/")
