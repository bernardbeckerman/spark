from __future__ import print_function
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

def localpath(path):
    return 'file://' + str(os.path.abspath(os.path.curdir)) + '/' + path

def isLine(line):
    return line.strip() != '' and line.strip().split()[0] == '<row' and line.strip().split()[-1] == '/>'

def postParse(line):
    root = etree.fromstring(line)
    dv = dict(root.items())
    if 'Tags' not in dv:
        return None
    tags = re.findall("\<(.*?)\>", dv['Tags'])
    return tags

inp = sc.textFile(localpath("../allPosts/*")) \
        .map(lambda x: x.encode("ascii", "ignore")) \
        .filter(isLine) \
        .map(postParse) \
        .filter(lambda x: x is not None)

model    = Word2Vec().setVectorSize(100).setSeed(42).fit(inp)
synonyms = model.findSynonyms('ggplot2', 25)
for word, cosine_distance in synonyms:
    print("{}: {}".format(word, cosine_distance))
