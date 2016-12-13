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
    return line.strip() != '' and line.strip().split()[0] == '<row' and line.strip().split()[-1] == '/>'

def userParse(line):
    root = etree.fromstring(line)
    dv = dict(root.items())
    return (dv["Id"], int(dv["Reputation"]))

def postParse(line):
    root = etree.fromstring(line)
    dv = dict(root.items())
    if 'OwnerUserId' not in dv:
        return None
    return (dv['OwnerUserId'], dv['PostTypeId'])

def calc_ratio(x):
    d = dict(Counter(list(x[1])))
    k1 = d.get('1',0)
    k2 = d.get('2',0)
    if (k1 + k2 != 0):
        return (x[0], float(k2)/(k2+k1))
    return (x[0], -1)

def agg_in(x,y):
    return (x[0] + y*(y!=-1), x[1] + (y!=-1))

def agg_out(x,y):
    return (x[0] + y[0], x[1] + y[1])

def ave_agg(v):
    if v[1] != 0:
        return (v[0]/v[1])
    return -1

users = sc.textFile(localpath("../stats/allUsers/*")) \
          .map(lambda x: x.encode("ascii", "ignore")) \
          .filter(isLine) \
          .map(userParse)

posts = sc.textFile(localpath("../stats/allPosts/*")) \
          .map(lambda x: x.encode("ascii", "ignore")) \
          .filter(isLine) \
          .map(postParse) \
          .filter(lambda x: x is not None) \
          .groupByKey() \
          .map(calc_ratio)

users_posts = users.join(posts) \
                   .collect()

#data.foreach(print)
#print data
count = 0
sum1 = 0
for entry in sorted(users_posts, key=lambda x: x[1][0]):
    count += 1
    sum1 += entry[1][1]
    print (str((entry[0],entry[1][1])) + ",")

print (sum1/count)
