from __future__ import print_function
import pickle
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from operator import add
from pyspark.mllib.feature import Word2Vec
from collections import Counter
from lxml import etree
import gzip
from pyspark import SparkContext
sc = SparkContext("local[*]", "temp")
import os
from collections import defaultdict
from dateutil import parser
import datetime
import re
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

def localpath(path):
    return 'file://' + str(os.path.abspath(os.path.curdir)) + '/' + path

def isLine(line):
    return line.strip() != '' and line.strip().split()[0] == '<row' and line.strip().split()[-1] == '/>'

def tagParse(line):
    root = etree.fromstring(line)
    dv = dict(root.items())
    if 'Tags' not in dv:
        return []
    tags = re.findall("\<(.*?)\>", dv['Tags'])
    return tags

def tagCheck(line):
    root = etree.fromstring(line[0])
    dv = dict(root.items())
    body = re.sub('\<.*?\>', ' ', dv['Body'])
    if 'Tags' not in dv:
        return None
    tags = re.findall("\<(.*?)\>", dv['Tags'])
    return (body, float(line[1] in tags))

#def tagParse(line):
#    root = etree.fromstring(line)
#    dv = dict(root.items())
#    if 'Tags' not in dv:
#        return None
#    tags = re.findall("\<(.*?)\>", dv['Tags'])
#    return tags

inp = sc.textFile(localpath("train/*")) \
        .map(lambda x: x.encode("ascii", "ignore")) \
        .filter(isLine) \
        .flatMap(tagParse) \
        .map(lambda x: (x,1))\
        .reduceByKey(add) \
        .collect()
tlist = []
for i in sorted(inp,key=lambda x: x[1])[-100:]:
    wd = i[0]
    train = sc.textFile(localpath("train/*")) \
              .map(lambda x: x.encode("ascii", "ignore")) \
              .filter(isLine) \
              .map(lambda x: (x,wd)) \
              .map(tagCheck) \
              .filter(lambda x: x is not None) \
              .toDF(['body','label'])
    test = sc.textFile(localpath("test/*")) \
             .map(lambda x: x.encode("ascii", "ignore")) \
             .filter(isLine) \
             .map(lambda x: (x,wd)) \
             .map(tagCheck) \
             .filter(lambda x: x is not None) \
             .toDF(['body','label'])
    tokenizer = Tokenizer(inputCol="body", outputCol="words")
    hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
    logreg = LogisticRegression(maxIter=10, regParam=0.01)
    pipeline = Pipeline(stages=[tokenizer, hashingTF, logreg])

    model = pipeline.fit(train)
    prediction = model.transform(test)
    selected = prediction.select("probability")
    plist = []
    for row in selected.collect():
        plist.append(row[0][0])
    tlist.append((wd,plist))
with open('plist.p', 'w') as f:
    pickle.dump(tlist,f)
import pdb; pdb.set_trace()
