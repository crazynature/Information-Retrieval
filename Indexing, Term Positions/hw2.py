from elasticsearch import Elasticsearch
from datetime import datetime
from math import log
import string
import nltk
import json
from ast import literal_eval
from nltk.stem.porter import PorterStemmer


class hw2:

    def __init__(self):
        self.stoplist = []
        f = open("stoplist.txt", "r")
        for line in f.readlines():
            line = line.strip('\n')
            self.stoplist.append(line)

    def invertedList_stemed(self):
        porter_stemmer = PorterStemmer()
        docList = []
        f = open("doclist_new_0609.txt", 'r')
        for line in f.readlines():
            line = line.strip()
            line = line.split(' ')

            if len(line) >= 3:
                line = line[2].split('-')[0]
                if line not in docList:
                    docList.append(line)
        # print(docList)
        fileName = 0
        fileCount = 0
        totalfileCount = 0
        offset = 0
        termCount = 0
        total = 0
        data = {}
        matchFile = {}
        docCount = 0
        isData = False
        iList = open("D:\\School\\CS6200\HW2\\list\\stem\\" + str(fileName) + ".txt", 'w')
        cat = open("D:\\School\\CS6200\HW2\\catalog\\stem\\" + str(fileName) + ".txt", 'w')
        doclen = open("D:\\School\\CS6200\HW2\\catalog\\stem\\" + str(fileName) + "Len.txt", 'w')
        docNoFile = open("D:\\School\\CS6200\HW2\\catalog\\stem\\" + str(fileName) + "docNo.txt", 'w')
        for docName in docList:
            f = open("D:\\School\\CS6200\\AP89_DATA\\AP_DATA\\ap89_collection\\" + docName, 'r')
            # f = open("file1.txt", 'r')
            for line in f.readlines():
                if not len(line):
                    continue
                if "<DOC>" in line:
                    count = 0
                    fileCount = fileCount + 1
                    totalfileCount += 1
                    print(totalfileCount)
                    # isData = False
                    if fileCount == 1000:
                        cat.writelines(str(len(data.keys())) + " " + str(total) + "\n")
                        for x in data.keys():
                            termCount += 1
                            out = ""
                            for y in data[x]:
                                temp = ""
                                for z in y:
                                    if len(temp) > 0:
                                        temp = temp + "," + str(z)
                                    else:
                                        temp = str(z)
                                if len(out) > 0:
                                    out = out + ";" + temp
                                else:
                                    out = temp
                            out = str(termCount) + " " + out + "!\n"
                            iList.writelines(out)
                            cat.writelines(
                                str(termCount) + " " + x + " " + str(matchFile[x]) + " " + str(offset) + "\n")
                            offset = offset + len(out) + 1
                        total = 0
                        fileCount = 0
                        fileName += 1
                        offset = 0
                        termCount = 0
                        iList.close()
                        cat.close()
                        iList = open("D:\\School\\CS6200\HW2\\list\\stem\\" + str(fileName) + ".txt", 'w')
                        cat = open("D:\\School\\CS6200\HW2\\catalog\\stem\\" + str(fileName) + ".txt", 'w')
                        data = {}
                        matchFile = {}
                if "<DOCNO>" in line:
                    docNo = line.split(" ")[1]
                    id = str(docCount)
                    docNoFile.writelines(id + " " + docNo+"\n")
                    docCount += 1
                if "</TEXT>" in line:
                    isData = False
                # print(data)
                if isData:
                    line = line.strip()
                    line = line.split(' ')
                    newLine = []
                    for num in range(0, len(line)):
                        newLine.append(porter_stemmer.stem(line[num]))
                    line = self.tokenize(line)
                    for x in newLine:
                        if x.lower() not in self.stoplist:
                            if x not in matchFile.keys():
                                matchFile[x] = 1
                            else:
                                matchFile[x] = matchFile[x] + 1
                            count += 1
                            if x not in data.keys():
                                data[x] = [[1, id, count]]
                            elif id in data[x][len(data[x]) - 1]:
                                newIndex = len(data[x]) - 1
                                temp = data[x][newIndex][0] + 1
                                data[x][newIndex].append(count)
                                data[x][newIndex].pop(0)
                                data[x][newIndex].insert(0, temp)
                            else:
                                data[x].append([1, id, count])
                        else:
                            count += 1
                if "<TEXT>" in line:
                    isData = True
                if "</DOC>" in line:
                    doclen.writelines(id + " " + str(count) + "\n")
                    total = total + count

            f.close()
            print(docName + " upload")
        doclen.close()
        docNoFile.close()
        print("input finished")

    def invertedList(self):
        docList = []
        f = open("doclist_new_0609.txt", 'r')
        for line in f.readlines():
            line = line.strip()
            line = line.split(' ')

            if len(line) >= 3:
                line = line[2].split('-')[0]
                if line not in docList:
                    docList.append(line)
        # print(docList)
        fileName = 0
        fileCount = 0
        totalfileCount = 0
        offset = 0
        termCount = 0
        total = 0
        data = {}
        matchFile = {}
        isData = False
        iList = open("D:\\School\\CS6200\HW2\\list\\" + str(fileName) + ".txt", 'w')
        cat = open("D:\\School\\CS6200\HW2\\catalog\\" + str(fileName) + ".txt", 'w')
        doclen = open("D:\\School\\CS6200\HW2\\catalog\\" + str(fileName) + "Len.txt", 'w')
        docNoFile = open("D:\\School\\CS6200\HW2\\catalog\\" + str(fileName) + "docNo.txt", 'w')
        docCount = 0
        for docName in docList:
            f = open("D:\\School\\CS6200\\AP89_DATA\\AP_DATA\\ap89_collection\\" + docName, 'r')
            # f = open("file1.txt", 'r')
            for line in f.readlines():
                if not len(line):
                    continue
                if "<DOC>" in line:
                    count = 0
                    fileCount = fileCount + 1
                    totalfileCount += 1
                    print(totalfileCount)
                    # isData = False
                    if fileCount == 1000:
                        cat.writelines(str(len(data.keys())) + " " + str(total) + "\n")
                        for x in data.keys():
                            termCount += 1
                            out = ""
                            for y in data[x]:
                                temp = ""
                                for z in y:
                                    if len(temp) > 0:
                                        temp = temp + "," + str(z)
                                    else:
                                        temp = str(z)
                                if len(out) > 0:
                                    out = out + ";" + temp
                                else:
                                    out = temp
                            out = str(termCount) + " " + out + "!\n"
                            iList.writelines(out)
                            cat.writelines(
                                str(termCount) + " " + x + " " + str(matchFile[x]) + " " + str(offset) + "\n")
                            offset = offset + len(out) + 1
                        total = 0
                        fileCount = 0
                        fileName += 1
                        offset = 0
                        termCount = 0
                        iList.close()
                        cat.close()
                        iList = open("D:\\School\\CS6200\HW2\\list\\" + str(fileName) + ".txt", 'w')
                        cat = open("D:\\School\\CS6200\HW2\\catalog\\" + str(fileName) + ".txt", 'w')
                        data = {}
                        matchFile = {}
                if "<DOCNO>" in line:
                    docNo = line.split(" ")[1]
                    id = str(docCount)
                    docNoFile.writelines(id + " " + docNo+"\n")
                    docCount += 1
                if "</TEXT>" in line:
                    isData = False
                # print(data)
                if isData:
                    line = line.strip()
                    # line = line.translate(str.maketrans('', '', string.punctuation))
                    line = line.split(' ')
                    line = self.tokenize(line)
                    for x in line:
                        if x.lower() not in self.stoplist:
                            if x not in matchFile.keys():
                                matchFile[x] = 1
                            else:
                                matchFile[x] = matchFile[x] + 1
                            count += 1
                            if x not in data.keys():
                                data[x] = [[1, id, count]]
                            elif id in data[x][len(data[x]) - 1]:
                                newIndex = len(data[x]) - 1
                                temp = data[x][newIndex][0] + 1
                                data[x][newIndex].append(count)
                                data[x][newIndex].pop(0)
                                data[x][newIndex].insert(0, temp)
                            else:
                                data[x].append([1, id, count])
                        else:
                            count += 1
                if "<TEXT>" in line:
                    isData = True
                if "</DOC>" in line:
                    doclen.writelines(id + " " + str(count) + "\n")
                    total = total + count

            f.close()
            print(docName + " upload")
        doclen.close()
        docNoFile.close()
        print("input finished")

    def tokenize(self, keywords):
        result = []
        for term in keywords:
            if term.endswith("'s"):
                result.append(term[:-2])
                result.append("'s")
            elif "." in term:
                if not term.endswith("."):
                    result.append(term)
                else:
                    result.append(term[:-1])
            elif "," in term:
                term = term.split(",")
                for x in term:
                    result.append(x)
            else:
                term = term.translate(str.maketrans('', '', string.punctuation))
                result.append(term)
        return result

    # self.mergeHelper(fileCount + 1)
    def mergeHelper_stemed(self, num):
        for x in range(0, num):
            self.merger_stemed(str(x))
            print(str(x) + " merged")

    def mergeHelper(self, num):
        for x in range(0, num):
            self.merger(str(x))
            print(str(x) + " merged")

    def merger_stemed(self, name):
        cat2 = open("D:\\School\\CS6200\HW2\\catalog\\stem\\" + name + ".txt", 'r')
        cat1 = open("D:\\School\\CS6200\HW2\\catalog\\stem\\" + "result.txt", 'r')
        file1 = open("D:\\School\\CS6200\HW2\\list\\stem\\" + "result.txt", 'r')
        file2 = open("D:\\School\\CS6200\HW2\\list\\stem\\" + name + ".txt", 'r')
        data1 = {}
        data2 = {}
        catalog1 = {}
        catalog2 = {}
        total = 0
        for line in cat1.readlines():
            line = line.strip()
            line = line.split(' ')
            if (len(line) > 3):
                catalog1[line[1]] = line
            else:
                total = total + int(line[1])
        for line in cat2.readlines():
            line = line.strip()
            line = line.split(' ')
            if (len(line) > 3):
                catalog2[line[1]] = line
            else:
                total = total + int(line[1])
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
        for line in file2.readlines():
            line = line.strip()
            temp = temp + line
            if temp[len(temp) - 1] is not "!":
                continue
            else:
                temp = temp[:-1]
                temp = temp.split(" ")
                data2[temp[0]] = temp[1]
                temp = ""
        result = {}
        resultCat = {}
        for x in catalog1.keys():
            if x in catalog2.keys():
                temp = data1[catalog1[x][0]] + ";" + data2[catalog2[x][0]]
                result[x] = temp
                resultCat[x] = int(catalog1[x][2]) + int(catalog2[x][2])
            else:
                result[x] = data1[catalog1[x][0]]
                resultCat[x] = catalog1[x][2]
        for x in catalog2.keys():
            if x not in catalog1.keys():
                result[x] = data2[catalog2[x][0]]
                resultCat[x] = catalog2[x][2]
        cat1.close()
        cat2.close()
        file1.close()
        file2.close()
        cat1 = open("D:\\School\\CS6200\HW2\\catalog\\stem\\" + "result.txt", 'w')
        file1 = open("D:\\School\\CS6200\HW2\\list\\stem\\" + "result.txt", 'w')
        count = 1
        length = 0
        cat1.write(str(len(resultCat.keys())) + " " + str(total) + "\n")
        for x in result.keys():
            temp = str(count) + " " + result[x] + "!\n"
            tempCat = str(count) + " " + x + " " + str(resultCat[x]) + " " + str(length) + "\n"
            cat1.write(tempCat)
            file1.write(temp)
            length = length + len(temp) + 1
            count += 1
        cat1.close()
        file1.close()

    def merger(self, name):
        cat2 = open("D:\\School\\CS6200\HW2\\catalog\\" + name + ".txt", 'r')
        cat1 = open("D:\\School\\CS6200\HW2\\catalog\\" + "result.txt", 'r')
        file1 = open("D:\\School\\CS6200\HW2\\list\\" + "result.txt", 'r')
        file2 = open("D:\\School\\CS6200\HW2\\list\\" + name + ".txt", 'r')
        data1 = {}
        data2 = {}
        catalog1 = {}
        catalog2 = {}
        total = 0
        for line in cat1.readlines():
            line = line.strip()
            line = line.split(' ')
            if (len(line) > 3):
                catalog1[line[1]] = line
            else:
                total = total + int(line[1])
        for line in cat2.readlines():
            line = line.strip()
            line = line.split(' ')
            if (len(line) > 3):
                catalog2[line[1]] = line
            else:
                total = total + int(line[1])
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
        for line in file2.readlines():
            line = line.strip()
            temp = temp + line
            if temp[len(temp) - 1] is not "!":
                continue
            else:
                temp = temp[:-1]
                temp = temp.split(" ")
                data2[temp[0]] = temp[1]
                temp = ""
        result = {}
        resultCat = {}
        for x in catalog1.keys():
            if x in catalog2.keys():
                temp = data1[catalog1[x][0]] + ";" + data2[catalog2[x][0]]
                result[x] = temp
                resultCat[x] = int(catalog1[x][2]) + int(catalog2[x][2])
            else:
                result[x] = data1[catalog1[x][0]]
                resultCat[x] = catalog1[x][2]
        for x in catalog2.keys():
            if x not in catalog1.keys():
                result[x] = data2[catalog2[x][0]]
                resultCat[x] = catalog2[x][2]
        cat1.close()
        cat2.close()
        file1.close()
        file2.close()
        cat1 = open("D:\\School\\CS6200\HW2\\catalog\\" + "result.txt", 'w')
        file1 = open("D:\\School\\CS6200\HW2\\list\\" + "result.txt", 'w')
        count = 1
        length = 0
        cat1.write(str(len(resultCat.keys())) + " " + str(total) + "\n")
        for x in result.keys():
            temp = str(count) + " " + result[x] + "!\n"
            tempCat = str(count) + " " + x + " " + str(resultCat[x]) + " " + str(length) + "\n"
            cat1.write(tempCat)
            file1.write(temp)
            length = length + len(temp) + 1
            count += 1
        cat1.close()
        file1.close()

    def mergeList(self, name):
        cat2 = open("D:\\School\\CS6200\HW2\\catalog\\" + name + ".txt", 'r')
        cat1 = open("D:\\School\\CS6200\HW2\\catalog\\" + "result.txt", 'r')
        file1 = open("D:\\School\\CS6200\HW2\\list\\" + "result.txt", 'r')
        file2 = open("D:\\School\\CS6200\HW2\\list\\" + name + ".txt", 'r')
        data1 = {}
        data2 = {}
        total = 0
        result = {}
        resultCat = {}
        key1 = []
        key2 = []
        for line in cat1.readlines():
            line = line.strip()
            line = line.split(' ')
            if (len(line) > 3):
                data1[line[1]] = line
                key1.append(line[1])
            else:
                total = total + int(line[1])
        for line in cat2.readlines():
            line = line.strip()
            line = line.split(' ')
            if (len(line) > 3):
                data2[line[1]] = line
                key2.append(line[1])
            else:
                total = total + int(line[1])
        count = 0
        length = 0
        if len(key1) > 1:
            for x in key1:
                count += 1
                if x in key2:
                    file1.seek(int(data1[x][3], 0))
                    file2.seek(int(data2[x][3], 0))
                    temp1 = ""
                    # temp1 = file1.read(int(data1[key1[count]][3])-int(data1[x][3])).strip()
                    temp = file1.readline()
                    if "!" in temp:
                        temp1 = temp
                    while "!" not in temp:
                        temp1 = temp1 + temp.strip()
                        temp = file1.readline()
                    temp1 = temp1.strip()
                    temp1 = temp1.split(" ")
                    temp2 = ""
                    temp = file2.readline()
                    if "!" in temp:
                        temp2 = temp
                    while "!" not in temp:
                        temp2 = temp2 + temp.strip()
                        temp = file2.readline()
                    temp2 = temp2.strip()
                    temp2 = temp2.split(" ")
                    if len(temp1) < 2:
                        temp2 = temp2[1]
                        result[x] = str(count) + " " + temp2
                        resultCat[x] = str(count) + " " + x + " " + str(int(data2[x][2])) + " " + str(
                            length)
                    else:
                        temp1 = temp1[1]
                        temp2 = temp2[1]
                        result[x] = str(count) + " " + temp1 + ";" + temp2
                        resultCat[x] = str(count) + " " + x + " " + str(
                            int(data1[x][2]) + int(data2[x][2])) + " " + str(
                            length)
                    length = length + len(str(result[x])) + 1
                else:
                    file1.seek(int(data1[x][3], 0))
                    temp1 = ""
                    # temp1 = file1.read(int(data1[key1[count]][3])-int(data1[x][3])).strip()
                    temp = file1.readline()
                    if "!" in temp:
                        temp1 = temp
                    while "!" not in temp:
                        temp1 = temp1 + temp.strip()
                        temp = file1.readline()
                    temp1 = temp1.strip()
                    temp1 = temp1.split(" ")
                    if len(temp1) < 2:
                        continue
                    temp1 = temp1[1]
                    result[x] = str(count) + " " + temp1
                    resultCat[x] = str(count) + " " + x + " " + str(data1[x][2]) + " " + str(length)
                    length = length + len(str(result[x])) + 1

        print(count)
        for x in key2:
            if x not in key1:
                count += 1
                file2.seek(int(data2[x][3], 0))
                temp2 = ""
                temp = file2.readline()
                if "!" in temp:
                    temp2 = temp
                else:
                    while "!" not in temp:
                        temp2 = temp2 + temp.strip()
                        temp = file2.readline()
                temp2 = temp2.strip()
                temp2 = temp2.split(" ")
                temp2 = temp2[1]
                result[x] = str(count) + " " + temp2
                resultCat[x] = str(count) + " " + x + " " + str(data2[x][2]) + " " + str(length)
                length = length + len(str(result[x])) + 1
        file2.close()
        file1.close()
        cat1.close()
        cat2.close()
        cat1 = open("D:\\School\\CS6200\HW2\\catalog\\" + "result.txt", 'w')
        file1 = open("D:\\School\\CS6200\HW2\\list\\" + "result.txt", 'w')
        cat1.write(str(len(result.keys())) + " " + str(total) + "\n")
        for x in key2:
            cat1.writelines(str(resultCat[x]) + "\n")
            file1.writelines(str(result[x]) + "\n")


if __name__ == "__main__":
    test = hw2()

    # cat1 = open("D:\\School\\CS6200\HW2\\catalog\\" + "result.txt", 'w')
    # file1 = open("D:\\School\\CS6200\HW2\\list\\" + "result.txt", 'w')
    # cat1.write("")
    # file1.write("")
    # cat1.close()
    # file1.close()
   # test.invertedList()
    test.mergeHelper(85)
    test.invertedList_stemed()
    test.mergeHelper_stemed(85)
