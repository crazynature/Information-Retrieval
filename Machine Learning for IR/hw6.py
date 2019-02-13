import sys
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
import random
import math
import sys
import getopt
from scipy import *
from liblinearutil import *


class hw6:
    def getMatrix(self, query=False):

        f = open("data/qrels.adhoc.51-100.AP89.txt", 'r')
        ql = {}
        ret = {}
        for line in f.readlines():
            line = line.strip()
            temp = line.split(' ')
            if temp[0] not in ql.keys():
                ql[temp[0]] = {}
                ql[temp[0]][temp[2]] = temp[3]
            else:
                ql[temp[0]][temp[2]] = temp[3]

        # print(ql)
        # print(ret)
        f.close()
        tf = {}
        f = open('data/TF.txt', 'r')
        for line in f.readlines():
            line = line.strip()
            temp = line.split((' '))
            if temp[0] not in tf.keys():
                tf[temp[0]] = {}
                tf[temp[0]][temp[2]] = temp[4]
            else:
                tf[temp[0]][temp[2]] = temp[4]
        f.close()

        tfidf = {}
        f = open('data/TFIDF.txt', 'r')
        for line in f.readlines():
            line = line.strip()
            temp = line.split((' '))
            if temp[0] not in tfidf.keys():
                tfidf[temp[0]] = {}
                tfidf[temp[0]][temp[2]] = temp[4]
            else:
                tfidf[temp[0]][temp[2]] = temp[4]
        f.close()
        bm25 = {}
        f = open('data/BM25.txt', 'r')
        for line in f.readlines():
            line = line.strip()
            temp = line.split((' '))
            if temp[0] not in bm25.keys():
                bm25[temp[0]] = {}
                bm25[temp[0]][temp[2]] = temp[4]
            else:
                bm25[temp[0]][temp[2]] = temp[4]
        f.close()
        UnigramLM_L = {}
        f = open('data/UnigramLM_L.txt', 'r')
        for line in f.readlines():
            line = line.strip()
            temp = line.split((' '))
            if temp[0] not in UnigramLM_L.keys():
                UnigramLM_L[temp[0]] = {}
                UnigramLM_L[temp[0]][temp[2]] = temp[4]
            else:
                UnigramLM_L[temp[0]][temp[2]] = temp[4]
        f.close()
        UnigramLM_JM = {}
        f = open('data/UnigramLM_JM.txt', 'r')
        for line in f.readlines():
            line = line.strip()
            temp = line.split((' '))
            if temp[0] not in UnigramLM_JM.keys():
                UnigramLM_JM[temp[0]] = {}
                UnigramLM_JM[temp[0]][temp[2]] = temp[4]
            else:
                UnigramLM_JM[temp[0]][temp[2]] = temp[4]
        f.close()
        data = {}
        for k in bm25.keys():
            data[k] = {}
        for k in bm25.keys():
            for j in ql[k].keys():
                if j not in bm25[k].keys():
                    continue
                else:
                    if len(data[k]) <= 1000:
                        data[k][j] = [ql[k][j]]
            for j in bm25[k].keys():
                if j not in data[k].keys() and len(data[k]) <= 1000:
                    data[k][j] = ['0']
        for k in data.keys():
            for j in data[k].keys():
                if j in tf[k].keys():
                    data[k][j].append(tf[k][j])
                else:
                    data[k][j].append('0')
                if j in tfidf[k].keys():
                    data[k][j].append(tfidf[k][j])
                else:
                    data[k][j].append('0')
                if j in bm25[k].keys():
                    data[k][j].append(bm25[k][j])
                else:
                    data[k][j].append('0')
                if j in UnigramLM_L[k].keys():
                    data[k][j].append(UnigramLM_L[k][j])
                else:
                    data[k][j].append('0')
                if j in UnigramLM_JM[k].keys():
                    data[k][j].append(UnigramLM_JM[k][j])
                else:
                    data[k][j].append('0')
        print(data.keys())
        TEST_QUERIES = {56, 57, 64, 71, 99}
        f = open('testSet.txt', 'w')
        for k in TEST_QUERIES:
            k = str(k)
            for j in data[str(k)].keys():
                f.writelines(str(k) + ' ' + j + ' ' + data[k][j][0] + ' ' + data[k][j][1] + ' ' + data[k][j][2] + ' ' +
                             data[k][j][3] + ' ' + data[k][j][4] + ' ' + data[k][j][5] + '\n')
        f.close()
        f = open('trainSet.txt', 'w')
        for k in data.keys():
            if int(k) in TEST_QUERIES:
                continue
            else:
                for j in data[str(k)].keys():
                    f.writelines(k + ' ' + j + ' ' + data[k][j][0] + ' ' + data[k][j][1] + ' ' + data[k][j][2] + ' ' +
                                 data[k][j][3] + ' ' + data[k][j][4] + ' ' + data[k][j][5] + '\n')
        f.close()

    def creatData(self):
        f = open('testSet.txt', 'r')
        testSet = {}
        for line in f.readlines():
            line = line.strip()
            temp = line.split(' ')
            if temp[0] not in testSet:
                testSet[temp[0]] = [temp[1:]]
            else:
                testSet[temp[0]].append(temp[1:])
        f.close()
        f = open('trainSet.txt', 'r')
        trainSet = {}
        for line in f.readlines():
            line = line.strip()
            temp = line.split(' ')
            if temp[0] not in trainSet:
                trainSet[temp[0]] = [temp[1:]]
            else:
                trainSet[temp[0]].append(temp[1:])
        f.close()
        f = open('trainData.txt', 'w')
        for k in trainSet.keys():
            for j in trainSet[k]:
                if float(j[1]) > 0:
                    f.writelines(
                        str(1) + ' 1:' + j[2] + ' 2:' + j[3] + ' 3:' + j[4] + ' 4:' + j[5] + ' 5:' + j[6] + '\n')
                else:
                    f.writelines(
                        str(-1) + ' 1:' + j[2] + ' 2:' + j[3] + ' 3:' + j[4] + ' 4:' + j[5] + ' 5:' + j[6] + '\n')
        f.close()
        f = open('testData.txt', 'w')
        for k in testSet.keys():
            for j in testSet[k]:
                if float(j[1]) > 0:
                    f.writelines(
                        str(1) + ' 1:' + j[2] + ' 2:' + j[3] + ' 3:' + j[4] + ' 4:' + j[5] + ' 5:' + j[6] + '\n')
                else:
                    f.writelines(
                        str(-1) + ' 1:' + j[2] + ' 2:' + j[3] + ' 3:' + j[4] + ' 4:' + j[5] + ' 5:' + j[6] + '\n')
        f.close()

    def trainResult(self):
        y, x = svm_read_problem('trainData.txt')
        prob = problem(y, x)
        param = parameter('-s 3 -c 5 -q')
        m = train(prob, param)
        a, b, c = predict(y, x, m)
        # print(c)
        f = open('outputTrain.txt', 'w')
        for num in range(0, len(a)):
            f.writelines(str(a[num]) + ' ' + str(c[num][0]) + '\n')
        f.close()

        y, x = svm_read_problem('testData.txt')

        d, e, f = predict(y, x, m)
        z = open('outputTest.txt', 'w')
        for num in range(0, len(d)):
            z.writelines(str(d[num]) + ' ' + str(f[num][0]) + '\n')
        z.close()

    def getResult(self):
        x = open('outputTrain.txt', 'r')
        y = open('trainSet.txt', 'r')
        z = open('resultTest.txt', 'w')
        prob = []
        for line in x.readlines():
            line = line.strip()
            temp = line.split(' ')
            prob.append(temp[1])
        x.close()
        trainSet = {}
        count = 0
        for line in y.readlines():
            line = line.strip()
            temp = line.split(' ')
            if temp[0] not in trainSet.keys():
                trainSet[temp[0]] = [[temp[1], prob[count]]]
            else:
                trainSet[temp[0]].append([temp[1], prob[count]])
            count += 1
        for k in trainSet.keys():
            trainSet[k] = sorted(trainSet[k], key=lambda s: s[1], reverse=False)
        y.close()
        count = 0
        for k in trainSet.keys():
            for temp in trainSet[k]:
                count += 1
                z.writelines(str(k) + " Q0 " + str(temp[0]) + " " + str(count) + " " + str(temp[1]) + ' Exp\n')
        z.close()

        x = open('outputTest.txt', 'r')
        y = open('testSet.txt', 'r')
        z = open('resultTrain.txt', 'w')
        prob = []
        for line in x.readlines():
            line = line.strip()
            temp = line.split(' ')
            prob.append(temp[1])
        x.close()
        trainSet = {}
        count = 0
        for line in y.readlines():
            line = line.strip()
            temp = line.split(' ')
            if temp[0] not in trainSet.keys():
                trainSet[temp[0]] = [[temp[1], prob[count]]]
            else:
                trainSet[temp[0]].append([temp[1], prob[count]])
            count += 1
        for k in trainSet.keys():
            trainSet[k] = sorted(trainSet[k], key=lambda s: s[1], reverse=False)
        y.close()
        count = 0
        for k in trainSet.keys():
            for temp in trainSet[k]:
                count += 1
                z.writelines(str(k) + " Q0 " + str(temp[0]) + " " + str(count) + " " + str(temp[1]) + ' Exp\n')
        z.close()


if __name__ == "__main__":
    test = hw6()

    test.getMatrix()
    test.creatData()
    test.trainResult()
    test.getResult()
    # y, x = [1, -1], [[1, 0, 1], [-1, 0, -1]]
    # # Sparse data
    # y, x = [1, -1], [{1: 1, 3: 1}, {1: -1, 3: -1}]
    # prob = problem(y, x)
    # param = parameter('-s 0 -c 4 -B 1')
    # m = train(prob, param)
    # d, e, f = predict(y, x, m)
    # print(d, e, f)
