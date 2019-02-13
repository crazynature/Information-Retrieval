import sys
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
# es = Elasticsearch(['35.227.82.63:9200'])

class text_clean:

    def all_docs(self,index='hw3', doc_type='doc', window_size=10):
        es = Elasticsearch()
        # yields ID
        body = {
            'size': window_size,
            'query': {'match_all': {}}
        }
        res = es.search(index=index, doc_type=doc_type, scroll='1m', body=body)
        count = len(res['hits']['hits'])

        while True:
            for d in res['hits']['hits']:
                soup = BeautifulSoup(d["_source"]['raw'], 'html.parser')
                id = d['_id']
            data = ""
            for x in range(0, len(soup.find_all('p'))):
                data = data + test.getCleanText(str(soup.find_all('p')[x]))
            # for table in soup.find_all('table'):
            #     for line in table.findAll('tr'):
            #         for l in line.findAll('td'):
            #             if l.find('sup'):
            #                 l.find('sup').extract()
            #             data = data + test.getCleanText(str(l))
            body = {"url": d["_source"]['url'], "inlink": d["_source"]['inlinks'], "outlinks": d["_source"]['outlinks'],
                    "text": data, "raw": d["_source"]['raw']}
            es.index(index=index, doc_type=doc_type, id=id, body=body)
            total = res['hits']['total']
            print(id)
            print('[{}/{}({}%)]'.format(count, total, round((count / total) * 100, 2)))
            sys.stdout.flush()
            if total <= count: break
            scrollId = res['_scroll_id']
            res = es.scroll(scroll_id=scrollId, scroll='1m')
            count += len(res['hits']['hits'])

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
        if result.endswith("\n"):
            result = result[:-1]
        return result

if __name__ == "__main__":
    test = text_clean()
    index = 'ap_dataset'
    doc_type = 'document'
    window_size = 10
    fields = ['raw']
    test.all_docs(index,doc_type,window_size)
