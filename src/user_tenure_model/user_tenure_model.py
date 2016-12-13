from __future__ import print_function
from collections import Counter
from lxml import etree
import gzip
from pyspark import SparkContext
sc = SparkContext("local[*]", "temp")
import os
from collections import defaultdict
import datetime
from dateutil import parser

def localpath(path):
    return 'file://' + str(os.path.abspath(os.path.curdir)) + '/' + path

def isLine(line):
    return line.strip() != '' and line.strip().split()[0] == '<row' and line.strip().split()[-1] == '/>'

def qParse(line):
    root = etree.fromstring(line)
    dv = dict(root.items())
    if dv['PostTypeId'] == '1' and 'AcceptedAnswerId' in dv:
        return (dv['AcceptedAnswerId'], parser.parse(dv['CreationDate']))
    return None

def aParse(line):
    root = etree.fromstring(line)
    dv = dict(root.items())
    if dv['PostTypeId'] == '2':
        return (dv['Id'],               parser.parse(dv['CreationDate']))
    return None

def calc_ratio(x):
    d = dict(Counter(list(x)))
    kT = d.get(True,0)
    kF = d.get(False,0)
    return (float(kT)/(kF + kT))

questions = sc.textFile(localpath("../allPosts/*")) \
              .map(lambda x: x.encode("ascii", "ignore")) \
              .filter(isLine) \
              .map(qParse) \
              .filter(lambda x: x is not None)

answers   = sc.textFile(localpath("../allPosts/*")) \
              .map(lambda x: x.encode("ascii", "ignore")) \
              .filter(isLine) \
              .map(aParse) \
              .filter(lambda x: x is not None)

t_answer  = questions.join(answers) \
                     .map(lambda x: (x[1][0].hour, (x[1][1]-x[1][0]).total_seconds() < 10800)) \
                     .groupByKey() \
                     .mapValues(calc_ratio) \
                     .collect()

#data.foreach(print)
#print data
count = 0
sum1 = 0
for entry in sorted(t_answer, key=lambda x: x[0]):
    print (str(entry[1]) + ",")
