from __future__ import print_function
from collections import Counter
from lxml import etree
import gzip
from pyspark import SparkContext
sc = SparkContext("local[*]", "temp")
import os
from collections import defaultdict
from dateutil import parser
import datetime

stt_date = parser.parse('Jan 1 1970')

def localpath(path):
    return 'file://' + str(os.path.abspath(os.path.curdir)) + '/' + path

def isLine(line):
    return line.strip() != '' and line.strip().split()[0] == '<row' and line.strip().split()[-1] == '/>'

def postParse(line):
    root = etree.fromstring(line)
    dv = dict(root.items())
    salientKeys = ('Score', 'ViewCount', 'AnswerCount', 'FavoriteCount', 'CreationDate', 'PostTypeId')
    if 'OwnerUserId' not in dv:
        return None
    return (dv["OwnerUserId"], dict([(k,dv.get(k,0)) for k in salientKeys]))

def tagParse(x):
    min_date = (datetime.datetime.now() - stt_date).total_seconds()
    dates = []
    d100  = 100*24*60*60
    d150  = 150*24*60*60
    firstQ = False
    for dv1 in x[1]:
        dv = dv1[0]
        cd = dv1[1]
        date = (parser.parse(dv['CreationDate']) - stt_date).total_seconds()
        acc_date = cd
        if (date < min_date and int(dv['PostTypeId']) == 1):
            dv_first = dv
            min_date = date
            firstQ = True
        dates.append(date)
    isVet = False
    for date in dates:
        diff_date = date - acc_date
        if (diff_date > d100 and diff_date <= d150):
            isVet = True
            break
    salientKeys = ('Score', 'ViewCount', 'AnswerCount', 'FavoriteCount')
    if firstQ:
        return (isVet, [float(dv_first.get(k,0)) for k in salientKeys])
    return None

def userParse(line):
    root = etree.fromstring(line)
    dv = dict(root.items())
    if 'CreationDate' in dv:
        return ((dv["Id"], (parser.parse(dv['CreationDate']) - stt_date).total_seconds()))
    return None

def agg_in(x,y):
    return (x[0] + 1, (x[1][0] + y[0], x[1][1] + y[1], x[1][2] + y[2], x[1][3] + y[3]))

def agg_out(x,y):
    return (x[0] + y[0], (x[1][0] + y[1][0], x[1][1] + y[1][1], x[1][2] + y[1][2], x[1][3] + y[1][3]))

def ave_agg(v):
    if v[0] != 0:
        return (v[1][0]/v[0],v[1][1]/v[0],v[1][2]/v[0],v[1][3]/v[0])
    return None

users = sc.textFile(localpath("../allUsers/*")) \
          .map(lambda x: x.encode("ascii", "ignore")) \
          .filter(isLine) \
          .map(userParse) \
          .filter(lambda x: x is not None)

stats = sc.textFile(localpath("../allPosts/*")) \
          .map(lambda x: x.encode("ascii", "ignore")) \
          .filter(isLine) \
          .map(postParse) \
          .filter(lambda x: x is not None) \
          .join(users) \
          .groupByKey() \
          .map(tagParse) \
          .filter(lambda x: x is not None) \
          .aggregateByKey((0,(0,0,0,0)), agg_in, agg_out) \
          .mapValues(ave_agg) \
          .collect()

print (stats)

#data.foreach(print)
#print data
#count = 0
#sum1 = 0

#for entry in stats:
#    print(entry)
#    count += 1
#    sum1 += entry[1][1]
#    print (str((entry[0],entry[1][1])) + ",")
#
#print (sum1/count)
