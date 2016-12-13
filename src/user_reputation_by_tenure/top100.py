from __future__ import print_function
from collections import Counter
from lxml import etree
import gzip
from pyspark import SparkContext
sc = SparkContext("local[*]", "temp")
import os
from collections import defaultdict
from dateutil import parser

def localpath(path):
    return 'file://' + str(os.path.abspath(os.path.curdir)) + '/' + path

def isLine(line):
    return line.strip() != '' and line.strip().split()[0] == '<row' and line.strip().split()[-1] == '/>'

def postParse(line):
    root = etree.fromstring(line)
    dv = dict(root.items())
    salient_keys = ('Score','ViewCount','AnswerCount','FavoriteCount','CreationDate')
    if 'OwnerUserId' not in dv:
        return None
    return (dv['OwnerUserId'], dict((k,dv[k]) for k in salient_keys))

def userParse(x):
    dates    = []
    stt_date = parser.parse('Jan 1 1970')
    min_date = parser.parse('Jan 1 1970')
    100days  = 100*24*60*60
    150days  = 150*24*60*60
    for dv in x[1]:
        date = (parser.parse(dv['CreationDate']) - stt_date).total_seconds()
        dates.append(date)
        if date < min_date:
            min_date = date
            dv_first = dv
    isVet = False
    for date in dates:
        diff_date = date - min_date
        if (diff_date > 100days and diff_date <= 150days):
            isVet = True
            break
    salientKeys = ('Score','ViewCount','AnswerCount','FavoriteCount')
    return (isVet, (dv_first[k]) for k in salientKeys if k in dv_first))

def agg_in(x,y):
    return (x[0] + 1, (x[1][0] + y[1][0], x[1][1] + y[1][1], x[1][2] + y[1][2], x[1][3] + y[1][3]))

def agg_out(x,y):
    return (x[0] + y[0], (x[1][0] + y[1][0], x[1][1] + y[1][1], x[1][2] + y[1][2], x[1][3] + y[1][3]))

def ave_agg(v):
    if v[1] != 0:
        return (v[1][0]/v[0],v[1][1]/v[0],v[1][2]/v[0],v[1][3]/v[0])
    return None

posts = sc.textFile(localpath("../stats/allPosts/*")) \
          .map(lambda x: x.encode("ascii", "ignore")) \
          .filter(isLine) \
          .map(postParse) \
          .groupByKey() \
          .map(userParse) \
          .groupByKey() \
          .aggregateByKey((0,0), agg_in, agg_out) \
          .mapValues(ave_agg) \
          .collect()

#data.foreach(print)
#print data
count = 0
sum1 = 0
for entry in sorted(users_posts, key=lambda x: x[0]):
    print (str((entry[0],entry[1])) + ",")
