import sys
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
# es = Elasticsearch(['35.227.82.63:9200'])
import random
import math
import sys
import getopt
import matplotlib.pyplot as plt

class hw5:
    def getPlot(self,rec,pre):

        for k in rec.keys():
            y = []
            x = []
            for i in rec[k]:
            # tempRec = 0
            # tempPre = 0

            #     tempRec += rec[k][i]
            #     tempPre += pre[k][i]
            #
            # tempRec = tempRec / len(rec.keys())
            # tempPre = tempPre / len(rec.keys())
                x.append(i)
            for i in pre[k]:
                y.append(i)
            plt.figure()
            plt.plot(x,y)
            plt.xlabel("Recall")
            plt.ylabel("Precision")
            plt.title("Recall-Precision plot")
            plt.savefig("hw5/Recall-Precision_"+k+".png")

    def getHW1(self,query=False):
        f= open("hw5/qrels.adhoc.51-100.AP89.txt",'r')
        ql = {}
        ret={}
        for line in f.readlines():
            line=line.strip()
            temp = line.split(' ')
            if temp[2] not in ql.keys():
                ql[temp[2]] = [[temp[0], temp[3]]]
            else:
                ql[temp[2]].append([temp[0], temp[3]])
            if float(temp[3])>0:
                if temp[0] not in ret.keys():
                    ret[temp[0]]=1
                else:
                    ret[temp[0]]+=1
        # print(ql)
        # print(ret)
        f.close()
        data={}
        f=open('hw5/BM25.txt', 'r')
        count = 0
        for line in f.readlines():
            t = count
            line=line.strip()
            temp = line.split(' ')
            if temp[2] not in ql.keys():
                if temp[0] not in data:
                    data[temp[0]] = [[temp[2], '0']]
                    count += 1
                else:
                    data[temp[0]].append([temp[2], '0'])
                    count += 1
            else:
                if len(ql[temp[2]])>1:
                    for k in ql[temp[2]]:
                        if int(temp[0]) == int(k[0]):
                            if temp[0] not in data:
                                data[temp[0]]=[[temp[2],k[1]]]
                                count += 1
                            else:
                                data[temp[0]].append([temp[2],k[1]])
                                count += 1

                    if t == count:
                        if temp[0] not in data:
                            data[temp[0]] = [[temp[2], '0']]
                            count += 1
                        else:
                            data[temp[0]].append([temp[2], '0'])
                            count += 1
                else:
                    if temp[0] not in data:
                        if int(temp[0]) == int(ql[temp[2]][0][0]):
                            data[temp[0]] = [[temp[2], ql[temp[2]][0][1]]]
                        else:
                            data[temp[0]] = [[temp[2], '0']]
                        count += 1

                    else:
                        if int(temp[0]) == int(ql[temp[2]][0][0]):
                            data[temp[0]].append([temp[2], ql[temp[2]][0][1]])
                        else:
                            data[temp[0]].append([temp[2], '0'])
                        count += 1

            if t == count:
                print(str(temp[0]))
                print(ql[temp[2]])
        f.close()


        rec = {}
        pre = {}
        average = {}
        f1 = {}
        rpre = {}
        ndcg = {}
        for k in data.keys():

            rec[k] = self.recall(data[k])
            pre[k] = self.precision(data[k])
            print(k+' '+str(ret[k]))
            average[k] = self.averageP(data[k], ret[k])
            rpre[k] = self.rPrec(rec[k], pre[k])
            ndcg[k] = self.nDcg(data[k])
            f1[k] = self.getF1(pre[k], rec[k])
        # self.getPlot(rec,pre)

        if query:
            for k in data.keys():
                print("Query " + k)
                for i in [4, 9, 19, 49, 99,999]:
                    print("recall at rank " + str(i+1) + ' is ' + str(rec[k][i]))
                    print("precision at rank " + str(i+1) + ' is ' + str(pre[k][i]))
                    print("F1 at rank " + str(i+1) + ' is ' + str(f1[k][i]))
                    print("average precision at rank " + str(i+1) + ' is ' + str(average[k][i]))
                    print("nDCG at rank " + str(i+1) + ' is ' + str(ndcg[k][i]))
                print("R precision at rank " + str(i+1) + ' is ' + str(rpre[k]))
                print('\n')

        else:
            for i in [4, 9, 19, 49, 99,199,999]:
                tempRec = 0
                tempPre = 0
                tempAver = 0
                tmepNDOG = 0
                tempRPRE = 0
                tempF1 = 0
                for k in data.keys():
                    tempRec += rec[k][i]
                    tempPre += pre[k][i]
                    tempAver += average[k][i]
                    tmepNDOG += average[k][i]
                    tempRPRE += rpre[k]
                    tempF1 += f1[k][i]
                tempRec = tempRec / len(data.keys())
                tempPre = tempPre / len(data.keys())
                tempAver = tempAver / len(data.keys())
                tmepNDOG = tmepNDOG / len(data.keys())
                tempRPRE = tempRPRE / len(data.keys())
                tempF1 = tempF1 / len(data.keys())
                print("recall at rank " + str(i+1) + ' is ' + str(tempRec))
                print("precision at rank " + str(i+1) + ' is ' + str(tempPre))
                print("F1 at rank " + str(i+1) + ' is ' + str(tempF1))
                print("average precision at rank " + str(i+1) + ' is ' + str(tempAver))
                print("nDCG at rank " + str(i+1) + ' is ' + str(tmepNDOG))
            print("R precision at rank " + str(i+1) + ' is ' + str(tempRPRE))

    def getRankResult(self, keywords, searchNo):
        hitDoc = []

        es = Elasticsearch(['35.227.82.63:9200'])
        if es.ping:
            print(1)

        body = {
            'size': 200,
            'query': {"match": {"text": keywords}}
        }
        body['_source'] = {'includes': ['url']}
        result = es.search(index='hw3', doc_type='doc', body=body, size=200)

        for x in result['hits']['hits']:
            hitDoc.append([searchNo, x['_source']['url'], x['_score']])

        f = open('hw5/result.txt', 'a')
        for z in hitDoc:
            f.writelines(str(z[0]) + ' ' + z[1] + ' '+str(z[2]) + '\n')
        f.close()
    def readQl(self, query=False):
        f = open('hw5/mergedqrel.txt', 'r')
        ql = {}
        ret={}
        for line in f.readlines():
            line = line.strip()
            temp = line.split(' ')
            mid =[temp[2],temp[3],temp[4]]
            mid = sorted(mid)
            if temp[1] not in ql.keys():
                ql[temp[1]] = [[temp[0], mid[1]]]
            else:
                ql[temp[1]].append([temp[0], mid[1]])
            if float(mid[1])>0:
                if temp[0] not in ret.keys():
                    ret[temp[0]]=1
                else:
                    ret[temp[0]]+=1

        f.close()
        # print(ql)

        data={}
        f=open('hw5/mergedqrel.txt', 'r')
        for line in f.readlines():
            line=line.strip()
            temp = line.split(' ')
            if len(ql[temp[1]])>1:
                for k in ql[temp[1]]:
                    if temp[0] is k[0]:
                        if temp[0] not in data:
                            data[temp[0]]=[temp[1],k[1]]
                        else:
                            data[temp[0]].append([temp[1],k[1]])
                        break
            else:
                if temp[0] not in data:
                    data[temp[0]] = [[temp[1], ql[temp[1]][0][1]]]
                else:
                    data[temp[0]].append([temp[1], ql[temp[1]][0][1]])
        f.close()
        # print(data)

        rec = {}
        pre = {}
        average = {}
        f1 = {}
        rpre = {}
        ndcg = {}
        for k in data.keys():
            rec[k] = self.recall(data[k])
            pre[k] = self.precision(data[k])
            average[k] = self.averageP(data[k], ret[k])
            rpre[k] = self.rPrec(rec[k], pre[k])
            ndcg[k] = self.nDcg(data[k])
            f1[k] = self.getF1(pre[k], rec[k])
        self.getPlot(rec,pre)
        if query:
            for k in data.keys():
                print("Query " + k)
                for i in [5, 10, 20, 50, 100]:
                    print("recall at rank " + str(i) + ' is ' + str(rec[k][i]))
                    print("precision at rank " + str(i) + ' is ' + str(pre[k][i]))
                    print("F1 at rank " + str(i) + ' is ' + str(f1[k][i]))
                    print("average precision at rank " + str(i) + ' is ' + str(average[k][i]))
                    print("nDCG at rank " + str(i) + ' is ' + str(ndcg[k][i]))
                print("R precision at rank " + str(i) + ' is ' + str(rpre[k]))
                print('\n')
        else:
            for i in [4, 9, 19, 49, 99,199]:
                tempRec = 0
                tempPre = 0
                tempAver = 0
                tmepNDOG = 0
                tempRPRE = 0
                tempF1 = 0
                for k in data.keys():
                    tempRec += rec[k][i]
                    tempPre += pre[k][i]
                    tempAver += average[k][i]
                    tmepNDOG += average[k][i]
                    tempRPRE += rpre[k]
                    tempF1 += f1[k][i]
                tempRec = tempRec / len(data.keys())
                tempPre = tempPre / len(data.keys())
                tempAver = tempAver / len(data.keys())
                tmepNDOG = tmepNDOG / len(data.keys())
                tempRPRE = tempRPRE / len(data.keys())
                tempF1 = tempF1 / len(data.keys())
                print("recall at rank " + str(i+1) + ' is ' + str(tempRec))
                print("precision at rank " + str(i+1) + ' is ' + str(tempPre))
                print("F1 at rank " + str(i+1) + ' is ' + str(tempF1))
                print("average precision at rank " + str(i+1) + ' is ' + str(tempAver))
                print("nDCG at rank " + str(i+1) + ' is ' + str(tmepNDOG))
            print("R precision at rank " + str(i+1) + ' is ' + str(tempRPRE))

    def getF1(self, pre, rec):
        f1 = []
        for num in range(0, len(pre)):
            if (pre[num] + rec[num]) ==0:
                f1.append(0)
            else:
                f1.append(pre[num] * rec[num] / (pre[num] + rec[num]))
        return f1

    def nDcg(self, data):
        dcg = []
        sum = 0
        for num in range(0, len(data)):
            if float(data[num][1]) > 0:
                sum = sum + float(data[num][1]) / math.log(num + 2, 2)
            dcg.append(sum)
        sortedData = sorted(data, key=lambda s: s[1], reverse=True)
        ndcg = []
        sum = 0
        for num in range(0, len(dcg)):
            if float(sortedData[num][1]) > 0:
                sum = sum + float(sortedData[num][1]) / math.log(num + 2, 2)
            ndcg.append(dcg[num] / sum)
        # print(ndcg)
        return ndcg

    def rPrec(self, rec, pre):
        min = abs(rec[0] - pre[0])
        record = 0
        rpre = 0
        for num in range(0, len(rec)):
            if rec[num] == pre[num]:
                rpre = rec[num]
                min = 0
                break
            else:
                diff = abs(rec[0] - pre[0])
                if diff < min:
                    min = diff
                    record = num
        if min != 0:
            return rec[record]
        # print(rpre)
        return rpre

    def recall(self, data):
        count = 0
        # print(data)
        for d in data:
            if float(d[1]) > 0:
                count += 1
        recal = []
        temp = 0
        for k in data:
            if float(k[1]) > 0:
                temp += 1
            recal.append(temp / count)
        # print(recal)

        return recal

    def precision(self, data):
        count = 0
        pre = []
        for num in range(0, len(data)):
            if float(data[num][1]) > 0:
                count += 1
            pre.append(count / (num + 1))
        # print(pre)
        return pre

    def averageP(self, data, total):
        count = 0
        average = []
        sum = 0

        # for d in data:
        #     if float(d[1]) > 0:
        #         total += 1
        for num in range(0, len(data)):
            if float(data[num][1]) > 0:
                count += 1
                sum+=(count/(num+1))
            average.append(sum/total)

        # print(average)
        return average

    def getQL(self, keywords, searchNo):
        hitDoc = []

        es = Elasticsearch(['35.227.82.63:9200'])
        if es.ping:
            print(1)

        body = {
            'size': 200,
            'query': {"match": {"text": keywords}}
        }
        body['_source'] = {'includes': ['url']}
        result = es.search(index='hw3', doc_type='doc', body=body, size=200)
        total = result['hits']['total']
        max = result['hits']['max_score']
        min = 10000
        for x in result['hits']['hits']:
            hitDoc.append([searchNo, x['_source']['url'], x['_score']])
            if min > x['_score']:
                min = x['_score']
        level0 = (max - min) / 5
        level1 = (max - min) / 3
        for y in hitDoc:
            diff = y[2] - min
            if diff < level0:
                y.append(0)
            elif diff < level1:
                y.append(1)
            else:
                y.append(2)
        f = open('hw5/' + searchNo + '.txt', 'w')
        for z in hitDoc:
            f.writelines(str(z[0]) + ' Jian_Cui' + ' ' + z[1] + ' '+str(z[2])+' ' + str(z[3]) + '\n')
        f.close()


if __name__ == "__main__":
    test = hw5()
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'q', ['query'])
    except getopt.GetoptError as err:
        print(str(err))
    if len(opts) == 0:
        test.readQl()
    for o, a in opts:
        if o in ("-q", "--query"):
            test.readQl(True)
        else:
            test.readQl()
    test.getHW1(True)
    test.getHW1()
    # test.getQL('ten commandments', '1')
    # test.getQL('College of Cardinals','2')
    # test.getQL('recent popes', '3')
    # test.getRankResult('ten commandments', '1')
    # test.getRankResult('College of Cardinals','2')
    # test.getRankResult('recent popes', '3')