from __future__ import print_function
from collections import Counter
from lxml import etree
import gzip
from pyspark import SparkContext
sc = SparkContext("local[*]", "temp")
import os
from collections import defaultdict

def localpath(path):
    return 'file://' + str(os.path.abspath(os.path.curdir)) + '/' + path

def isLine(line):
    return line.strip() != '' and line.strip().split()[0] == '<row'

def postParse(line):
    root = etree.fromstring(line)
    dv = dict(root.items())
    return (dv["PostId"], dv["VoteTypeId"])

def calc_ratio(x):
    d = dict(Counter(list(x[1])))
    k2 = d.get('2',0)
    k3 = d.get('3',0)
    k5 = d.get('5',0)
    if (k2 + k3 != 0):
        return (k5, float(k2)/(k2+k3))
    return (k5, -1)

def agg_in(x,y):
    return (x[0] + y*(y!=-1), x[1] + (y!=-1))

def agg_out(x,y):
    return (x[0] + y[0], x[1] + y[1])

def ave_agg(v):
    if v[1] != 0:
        return (v[0]/v[1])
    return -1

data = sc.textFile(localpath("stats/allVotes/*")) \
       .filter(isLine) \
       .map(parse) \
       .groupByKey() \
       .map(calc_ratio) \
       .aggregateByKey((0,0), agg_in, agg_out) \
       .mapValues(ave_agg) \
       .collect()


#data.foreach(print)
#print data
for entry in data:
    print (str((entry[0],entry[1])) + ",")
