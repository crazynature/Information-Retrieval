import sys
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
# es = Elasticsearch(['35.227.82.63:9200'])
import random
import math
class hw4:

    def __init__(self):
        self.inlinks = {}
        self.outlinks={}

    def convergence(self,data):
        result = 0
        for k in data.keys():
            result =result+(data[k]*math.log(data[k],2))
        result=-result
        return math.pow(2,result)

    def sortUrl(self, data,wt2g=True):
        sortedData = sorted(data, key=lambda s: s[1], reverse=True)
        result = []
        check=[]
        if wt2g:
            return sortedData
        for line in sortedData:
            temp = line
            temp = temp[0].split('//')[1]
            if temp not in check:
                result.append(line)
                check.append(temp)
        return result
    def all_docs(self,index='hw3', doc_type='doc', window_size=10):
        self.inlinks={}
        es = Elasticsearch(['35.227.82.63:9200'])
        if es.ping:
            print(1)
        body = {
            'size': window_size,
            'query': {'match_all': {}}
        }
        body['_source'] = {'includes':['url', 'inlinks','outlinks']}
        res = es.search(index=index, doc_type=doc_type, scroll='1m', body=body)
        count = len(res['hits']['hits'])
        no_outlinks=[]
        while True:
            for d in res['hits']['hits']:
                url = str(d["_source"]['url'])
                if 'inlinks' in d["_source"].keys():
                    inlinks= (d["_source"]['inlinks'])
                    self.inlinks[url]=inlinks
                else:
                    print(d['_id'])
                    self.inlinks[url]=[]
                if(len(d["_source"]['outlinks']))==0:
                    no_outlinks.append(url)
                self.outlinks[url]=len(d["_source"]['outlinks'])
                id = d['_id']
            total = res['hits']['total']
            print('[{}/{}({}%)]'.format(count, total, round((count / total) * 100, 2)))
            sys.stdout.flush()
            if total <= count: break
            scrollId = res['_scroll_id']
            res = es.scroll(scroll_id=scrollId, scroll='1m')
            count += len(res['hits']['hits'])

        self.pageRank(no_outlinks)

    def esBuildIn(self,keywords):
        hitDoc = []
        inlink = {}
        outlink ={}
        es = Elasticsearch(['35.227.82.63:9200'])
        if es.ping:
            print(1)
        body = {
            'size': 10,
            'query': {'match_all': {}}
        }
        body['_source'] = {'includes':['url', 'inlinks','outlinks']}
        res = es.search(index='hw3', doc_type='doc', scroll='1m', body=body)
        count = len(res['hits']['hits'])
        no_outlinks=[]
        while count<100000:
            for d in res['hits']['hits']:
                url = str(d["_source"]['url'])
                if 'inlinks' in d["_source"].keys():
                    inlinks= (d["_source"]['inlinks'])
                    inlink[url]=inlinks
                else:
                    print(d['_id'])
                    inlink[url]=[]
                outlink[url]=d["_source"]['outlinks']
                id = d['_id']
            total = res['hits']['total']
            print('[{}/{}({}%)]'.format(count, total, round((count / total) * 100, 2)))
            sys.stdout.flush()
            if total <= count: break
            scrollId = res['_scroll_id']
            res = es.scroll(scroll_id=scrollId, scroll='1m')
            count += len(res['hits']['hits'])
        body = {
            'size': 10,
            'query': {"match": {"text": keywords}}
        }
        body['_source'] = {'includes':['url', 'inlinks','outlinks']}
        result = es.search(index='hw3', doc_type='doc', body=body, size=2000)
        total = len(result['hits']['hits'])
        print(total)

        count - 0
        for num in range(0, total):
            if 'inlinks' in result['hits']['hits'][num]["_source"].keys():
                data = result['hits']['hits'][num]["_source"]['url']
                hitDoc.append(data)
                self.inlinks[data]=result['hits']['hits'][num]["_source"]['inlinks']
                self.outlinks[data]=result['hits']['hits'][num]["_source"]['inlinks']
                count +=1
            else:
                count -=1
            if count == 1000:
                break


        count = 0
        while len(hitDoc)< 10000:
            print(count)
            count+=1
            if len(hitDoc)>= 10000:
                break
            for page in self.outlinks.keys():
                if len(hitDoc)>= 10000:
                    break
                for current in self.outlinks[page]:
                    if current not in hitDoc:
                        hitDoc.append(current)
            for page in self.inlinks.keys():
                if len(hitDoc)>= 10000:
                    break
                if len(self.inlinks[page])<=200:
                    if page in outlink:
                        for i in outlink[page]:
                            if i not in hitDoc:
                                hitDoc.append(i)
                            if len(hitDoc)>= 10000:
                                break
                else:
                    added = []
                    for i in range(0,200):
                        num = random.randint(0,(len(inlink[page])-1))
                        if num not in added:
                            if inlink[page][num] not in hitDoc:
                                hitDoc.append(i)
                                if len(hitDoc)>= 10000:
                                    break
                            added.append(num)
                        else:
                            i -=1


        for page in hitDoc:
            if page in inlink.keys() and page not in self.inlinks.keys():
                self.inlinks[page]=inlink[page]
            if page in outlink.keys() and page not in self.outlinks.keys():
                self.outlinks[page]=outlink[page]
            if page not in inlink.keys():
                self.inlinks[page]=[]
            if page not  in outlink.keys():
                self.outlinks[page]=[]
        f=open("rootSet.txt",'w')
        for p in hitDoc:
            f.writelines(str(p)+'\n')
        f.close()
        self.hubRank(hitDoc)

    def hubRank(self,baseSet):
        hub={}
        auth={}
        for page in baseSet:
            hub[page]=1
        norm = 0
        for page in baseSet:
            authScore=0
            for temp in self.inlinks[page]:
                if temp in hub.keys():
                    authScore += hub[temp]
            norm +=math.pow(authScore,2)
            auth[page]=authScore
        norm = math.sqrt(norm)
        for k in auth.keys():
            auth[k]=auth[k]/norm
        count = 0
        oldCoveragehub=-1
        oldCoverageauth=-1
        difference=0
        while count <4:
            # count+=1
            print(count)
            norm = 0
            for page in baseSet:
                hubScore = 0
                for temp in self.outlinks[page]:
                    if temp in auth.keys():
                        hubScore+=auth[temp]
                hub[page]=hubScore
                norm += math.pow(hubScore,2)
            norm=math.sqrt(norm)
            for k in hub.keys():
                hub[k] = hub[k] / norm
            norm = 0
            for page in baseSet:
                authScore = 0
                for temp in self.inlinks[page]:
                    if temp in hub.keys():
                        authScore += hub[temp]
                auth[page] = authScore
                norm+=math.pow(authScore,2)
            norm = math.sqrt(norm)
            for k in auth.keys():
                auth[k] = auth[k] / norm
            difference = self.convergence(hub)-oldCoveragehub
            if difference<=1:
                difference = self.convergence(auth)-oldCoverageauth
                if difference<=1:
                    count+=1
                else:
                    count=0
            else:
                count=0
        hubResult=[]
        for k in hub.keys():
            hubResult.append([k,hub[k]])
        authResult=[]
        for k in auth.keys():
            authResult.append([k,auth[k]])
        hubResult=self.sortUrl(hubResult)
        authResult=self.sortUrl(authResult)
        f = open('hubResult.txt','w')
        for num in range(0,500):
            f.writelines(str(hubResult[num][0])+'    '+str(hubResult[num][1])+'\n')
        f.close()
        f = open('authResult.txt','w')
        for num in range(0,500):
            f.writelines(str(authResult[num][0])+'    '+str(authResult[num][1])+'\n')
        f.close()


    def wt2g(self):
        self.outlinks={}
        self.inlinks={}

        f= open('wt2g_inlinks.txt','r')
        total = 0
        for line in f.readlines():
            # line=line.strip()
            total+=1
            line=str.split(line)
            # self.inlinks[line[0]]=line[1:]
            tempList = []
            for num in range(0,len(line)):
                if line[0] not in self.inlinks.keys():
                    if num ==0:
                        temp = line[num]
                    else:
                        if line[num] not in tempList:
                            tempList.append(line[num])
                    self.inlinks[temp]=tempList
                else:
                    if num !=0:
                        if line[num] not in self.inlinks[line[0]]:
                            self.inlinks[line[0]].append(line[num])
            #         if line[num] in self.outlinks.keys():
            #             x = self.outlinks[line[num]]+1
            #             self.outlinks[line[num]]=x
            #         else:
            #             self.outlinks[line[num]]=1

        f.close()


        for page in self.inlinks.keys():
            for q in self.inlinks[page]:
                if q in self.outlinks.keys():
                    self.outlinks[q]+=1
                else:
                    self.outlinks[q]=1

        no_outlinks =[]
        for k in self.inlinks.keys():
            if k not in self.outlinks.keys():
                no_outlinks.append(k)
        self.pageRank(no_outlinks,'wt2gResult',True)

    def pageRank(self,no_outlinks,filename='result',wt2g =False):
        oldScore = {}
        newScore ={}
        total =len(self.inlinks)
        print(total)
        print(len(no_outlinks))
        for k in self.inlinks.keys():
            oldScore[k]=float(1)/float(total)

        count = 0
        oldCoverage =self.convergence(oldScore)
        while count<4:
            sinkPR=0.00
            for i in no_outlinks:
                sinkPR +=oldScore[i]
            for current in oldScore.keys():
                pageScore = 0.15/float(total)
                pageScore += (0.85*float(sinkPR)/float(total))
                for page in self.inlinks[current]:
                    if page in self.outlinks.keys():
                        pageScore +=(0.85*float(oldScore[page])/float(self.outlinks[page]))
                newScore[current]=pageScore
            for key in oldScore.keys():
                oldScore[key]=newScore[key]
            newScore={}
            temp = self.convergence(oldScore)
            difference = abs(temp-oldCoverage)
            print(difference)
            if difference<=1:
                count+=1
            else:
                count=0
            oldCoverage=temp
        result = []
        for key in oldScore.keys():
            result.append([key,oldScore[key]])
        f=open(filename+'.txt','w')
        if not wt2g:
            result = self.sortUrl(result)
            for num in range(0, 500):
                f.writelines(str(num) + ' ' + str(result[num]) +'\n')
        else:
            result = self.sortUrl(result)
            for num in range(0,500):
                if result[num][0] in self.outlinks.keys():
                    f.writelines(str(num)+' '+str(result[num])+' '+str(len(self.inlinks[result[num][0]]))+' '+str(self.outlinks[result[num][0]])+'\n')
                else:
                    f.writelines(
                        str(num) + ' ' + str(result[num]) + ' ' + str(len(self.inlinks[result[num][0]])) + ' ' + str(0) + '\n')
        f.close()






if __name__ == "__main__":
    test = hw4()
    # index = 'ap_dataset'
    # doc_type = 'document'
    # es = Elasticsearch(['35.227.82.63:9200'])
    # test.all_docs()
    test.wt2g()
    # test.esBuildIn('College of Cardinals')