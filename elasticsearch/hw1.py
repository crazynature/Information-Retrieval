from elasticsearch import Elasticsearch
from datetime import datetime
from math import log
import string
from nltk.stem.porter import PorterStemmer

class hw1:

    def __init__(self):
        self.stoplist = []
        f = open("stoplist.txt", "r")
        for line in f.readlines():
            line = line.strip('\n')
            self.stoplist.append(line)

    def sortAndPrind(self, data, searchNo, name):
        docNoFile = open("D:\\School\\CS6200\HW2\\catalog\\0docNo.txt", 'r')
        docName={}
        for line in docNoFile.readlines():
            line = line.split(" ")
            docName[line[0]]=line[1]
        sortedData = sorted(data, key=lambda s: s[1], reverse=True)
        f = open("search\\"+name  + '.txt', 'a+')
        if len(sortedData) > 1000:
            length = 1000
        else:
            length = len(sortedData)
        for num in range(0, length):
            text = searchNo + " Q0 " + docName[sortedData[num][0]] + " " + str(num + 1) + " " + str(
                sortedData[num][1]) + ' Exp'
            if sortedData[num][1] != 0:
                f.writelines(text)
        f.close()

    def proximity_search(self):
        f = open('query_proximity.txt', 'r')
        #    f = open("file1.txt", 'r')
        data = ""
        isData = False
        for line in f.readlines():
            line = line.strip()
            searchNo = line[0:3].strip()
            keywords = line[4:len(line)].strip()
            searchNo = searchNo.translate(str.maketrans('', '', string.punctuation))
            keywords = keywords.translate(str.maketrans('', '', string.punctuation))
            self.proximity_searchModel(searchNo, keywords)
    def proximity_searchModel(self, searchNo, keywords):
        porter_stemmer = PorterStemmer()
        keywords = keywords.translate(str.maketrans('', '', string.punctuation))
        temp2 = keywords.split(' ')
        temp = []
        for x in temp2:
            temp.append(porter_stemmer.stem(x))
        keywords = []
        for f in temp:
            if f not in self.stoplist:
                keywords.append(f)
        cat1 = open("D:\\School\\CS6200\HW2\\catalog\\stem\\" + "result.txt", 'r')
        file1 = open("D:\\School\\CS6200\HW2\\list\\stem\\" + "result.txt", 'r')
        len1 = open("D:\\School\\CS6200\HW2\\catalog\\stem\\" + "0Len.txt", 'r')
        data1 = {}
        catalog1 = {}
        length1 = {}
        total = 0
        for line in len1.readlines():
            line = line.strip();
            line = line.split(" ")
            length1[line[0]] = int(line[1])
        for line in cat1.readlines():
            line = line.strip()
            line = line.split(' ')
            if (len(line) > 3):
                catalog1[line[1]] = line
            else:
                voc = int(line[0])
                total = int(line[1])
        aveLen = total / len(length1.keys())
        temp = ""
        for line in file1.readlines():
            temp = temp + line.strip()
            if temp[len(temp) - 1] is not "!":
                continue
            else:
                temp = temp[:-1]
                temp = temp.split(" ")
                data1[temp[0]] = temp[1]
                temp = ""
        cat1.close()
        file1.close()
        len1.close()
        bm25Result = {}
        containTerm = {}
        for x in keywords:
            if x in catalog1.keys():
                freQ = self.countWordInQuery(x, keywords)
                temp = data1[catalog1[x][0]]
                temp = temp.split(";")
                numberOfMatch = len(temp)
                for y in temp:
                    y = y.split(",")
                    count = float(y[0])
                    docName = y[1]
                    sum = length1[docName]
                    bmScore = self.calculateBm25(count, sum, numberOfMatch, freQ)
                    if docName in bm25Result.keys():
                        bm25Result[docName] = bm25Result[docName] + bmScore
                    else:
                        bm25Result[docName] = bmScore
                    if docName in containTerm.keys():
                        if x not in containTerm[docName]:
                            temp = containTerm[docName]
                            temp.append(x)
                            containTerm[docName] = temp
                    else:
                        containTerm[docName]=[x]
        for docName in containTerm.keys():
            numOfContainTerms = len(containTerm[docName])
            lengthOfDocument = length1[docName]
            rangeMap = {}
            for term in containTerm[docName]:
                temp = data1[catalog1[term][0]]
                temp = temp.split(";")
                for matches in temp:
                    matches=matches.split(",")
                    if docName in matches:
                        numList = matches[2:]
                        rangeValue = []
                        for num in numList:
                            if num.isdigit():
                                rangeValue.append(int(num))
                        rangeMap[term]=rangeValue
            rangeOfWindow=self.getRangeOfWindow(rangeMap)
            pScore = (1500 - rangeOfWindow)*numOfContainTerms/(lengthOfDocument+voc)
            bm25Result[docName]=bm25Result[docName]+pScore
            print(docName+" search finished")
        bmout = []
        for x in bm25Result.keys():
            bmout.append((x, bm25Result[x]))
        self.sortAndPrind(bmout, searchNo, "ProximityResult")

    def getRangeOfWindow(self,rangeMap):
        matrix = []
        for x in rangeMap.keys():
            matrix.append(rangeMap[x])
        if len(matrix) ==1:
            return 0
        min =[]
        for x in range(0,len(matrix)):
            min.append(matrix[x][0])
        min = sorted(min)
        minValue=min[len(min)-1]-min[0]
        if len(matrix) == 2:
            x = 1
            y = 1
            while True:
                if x < len(matrix[0]):
                    min.pop()
                    min.insert(0,matrix[0][x])
                    min = sorted(min)
                    temp = min[len(min)-1]-min[0]
                    if temp < minValue:
                        minValue = temp
                    x = x + 1
                if y < len(matrix[1]):
                    min.pop(1)
                    min.append(matrix[1][y])
                    min = sorted(min)
                    temp = min[len(min)-1]-min[0]
                    if temp < minValue:
                        minValue = temp
                    y = y + 1
                if x ==len(matrix[0]) and y == len(matrix[1]):
                    return minValue
        else:
            x = 1
            y = 1
            z = 1
            while True:
                if x < len(matrix[0]):
                    min.pop()
                    min.insert(0, matrix[0][x])
                    min = sorted(min)
                    temp = min[len(min) - 1] - min[0]
                    if temp < minValue:
                        minValue = temp
                    x = x + 1
                if y < len(matrix[1]):
                    min.pop(1)
                    min.insert(1,matrix[1][y])
                    min = sorted(min)
                    temp = min[len(min) - 1] - min[0]
                    if temp < minValue:
                        minValue = temp
                    y = y + 1
                if z < len(matrix[2]):
                    min.pop(2)
                    min.append(matrix[2][z])
                    min = sorted(min)
                    temp = min[len(min) - 1] - min[0]
                    if temp < minValue:
                        minValue = temp
                    z = z + 1
                if x == len(matrix[0]) and y == len(matrix[1]) and z == len(matrix[2]):
                    return minValue

    def searchModel(self, searchNo, keywords):
        # porter_stemmer = PorterStemmer()
        keywords = keywords.translate(str.maketrans('', '', string.punctuation))
        # temp2 = keywords.split(' ')
        # temp = []
        # for x in temp2:
        #     temp.append(porter_stemmer.stem(x))
        temp = keywords.split(' ')
        keywords = []
        for f in temp:
            if f not in self.stoplist:
                keywords.append(f)
        # cat1 = open("D:\\School\\CS6200\HW2\\catalog\\stem\\" + "result.txt", 'r')
        # file1 = open("D:\\School\\CS6200\HW2\\list\\stem\\" + "result.txt", 'r')
        # len1 = open("D:\\School\\CS6200\HW2\\catalog\\stem\\" + "0Len.txt", 'r')
        cat1 = open("D:\\School\\CS6200\HW2\\catalog\\" + "result.txt", 'r')
        file1 = open("D:\\School\\CS6200\HW2\\list\\" + "result.txt", 'r')
        len1 = open("D:\\School\\CS6200\HW2\\catalog\\" + "0Len.txt", 'r')
        data1 = {}
        catalog1 = {}
        length1 = {}
        total = 0
        for line in len1.readlines():
            line = line.strip();
            line = line.split(" ")
            length1[line[0]] = int(line[1])
        for line in cat1.readlines():
            line = line.strip()
            line = line.split(' ')
            if (len(line) > 3):
                catalog1[line[1]] = line
            else:
                voc = int(line[0])
                total = int(line[1])
        aveLen = total / len(length1.keys())
        temp = ""
        for line in file1.readlines():
            temp = temp + line.strip()
            if temp[len(temp) - 1] is not "!":
                continue
            else:
                temp = temp[:-1]
                temp = temp.split(" ")
                data1[temp[0]] = temp[1]
                temp = ""
        cat1.close()
        file1.close()
        len1.close()
        tfResult = {}
        bm25Result = {}
        unResult = {}
        for x in keywords:
            if x in catalog1.keys():
                freQ = self.countWordInQuery(x, keywords)
                temp = data1[catalog1[x][0]]
                temp = temp.split(";")
                numberOfMatch = len(temp)
                for y in temp:
                    y = y.split(",")
                    count = float(y[0])
                    docName = y[1]
                    sum = length1[docName]
                    tfScore = count / (count + 0.5 + 1.5 * (sum / aveLen))
                    bmScore = self.calculateBm25(count, sum, numberOfMatch, freQ)
                    unSocre = (count + 1) / (sum + voc)
                    if unSocre != 0:
                        unSocre = log(unSocre)
                    if docName in tfResult.keys():
                        tfResult[docName] = tfResult[docName] + tfScore
                    else:
                        tfResult[docName] = tfScore
                    if docName in bm25Result.keys():
                        bm25Result[docName] = bm25Result[docName] + bmScore
                    else:
                        bm25Result[docName] = bmScore
                    if docName in unResult.keys():
                        unResult[docName] = unResult[docName] + unSocre
                    else:
                        unResult[docName] = unSocre
            print(x+" search finished")
        tfout = []
        for x in tfResult.keys():
            tfout.append((x, tfResult[x]))
        bmout = []
        for x in bm25Result.keys():
            bmout.append((x, bm25Result[x]))
        unout = []
        for x in unResult.keys():
            unout.append((x, unResult[x]))
        self.sortAndPrind(tfout, searchNo, "TF")
        self.sortAndPrind(bmout, searchNo, "BM25")
        self.sortAndPrind(unout, searchNo, "UnigramLM_L")

    def clear(self):
        f = open("search\\TF.txt", 'w')
        f.write("")
        f.close()
        f = open("search\\BM25" + '.txt', 'w')
        f.write("")
        f.close()
        f = open("search\\UnigramLM_L" +'.txt', 'w')
        f.write("")
        f.close()
    def strListContains(self, target, keywords):
        for f in keywords:
            if f in target:
                return True
        return False

    def calculateBm25(self, count, docSize, numberOfMatch, freQ):
        score = log((docSize + 0.5) / (numberOfMatch + 0.5)) * (count + 1.2 * count) / (
                count + 1.2 * ((1 - 0.75) + 0.75 * (docSize / numberOfMatch))) * (freQ + 100 * freQ) / (freQ + 100)
        return score

    def wordContains(self, target, keywords):
        for f in keywords:
            if f in target:
                return f
        return target

    def countWordInQuery(self, word, keywords):
        count = 0
        for f in keywords:
            if f is word:
                count += 1
        return count

    def search(self):
        f = open('query_desc.51-100.short.txt', 'r')
        #    f = open("file1.txt", 'r')
        data = ""
        isData = False
        for line in f.readlines():
            line = line.strip()
            searchNo = line[0:3].strip()
            keywords = line[4:len(line)].strip()
            searchNo = searchNo.translate(str.maketrans('', '', string.punctuation))
            keywords = keywords.translate(str.maketrans('', '', string.punctuation))
            self.searchModel(searchNo, keywords)


if __name__ == "__main__":
    test = hw1()
    #test.clear()
    #test.search()
    test.proximity_search()
