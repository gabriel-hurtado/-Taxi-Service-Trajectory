import random
import os
import re
from decimal import Decimal
import collections
import itertools
import math
import numpy
import folium
from folium import plugins
from random import randint

from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()
session.default_timeout = 9999.0
session.set_keyspace("e39_taxi")


 

 

 
def has_converged(mu, oldmu):
	#conv=True
	#differences=numpy.subtract(tuple(oldmu.values()), tuple(mu.values()))
	#for item in differences:
	#	if conv:
	#		conv=all(math.fabs(value) < 0.001 for value in item)
	return tuple(oldmu.values()) == tuple(mu.values()) #conv
    

def kmeansOffline(K,hoursConstraint):
    fetchQuerry='SELECT * FROM timesorted WHERE year>0 and month>0  and {0}'.format(hoursConstraint+ 'ALLOW FILTERING {0};')
    rows = iter(session.execute(fetchQuerry.format('limit 100')))   
    mu = getFirsts(rows,K)
    oldmu={}
    first=True
    while (first or not( has_converged(mu, oldmu))):
        rows = iter(fetchQuerry.format(''))   
        oldmu = mu
        S=[[0,0,0,0]]*K
        n=[0]*K
        x=getNextPoint(rows)
        while (x):
            bestmukey = getMinDist4D(x,mu)#classe la plus proche de x
            S[bestmukey]=numpy.sum([S[bestmukey],x],axis=0).tolist()
            n[bestmukey]+=1
            x=getNextPoint(rows)
        for i in range(0,K):
            if(n[i]!=0):
                mu[i]=numpy.divide(S[i], n[i]).tolist()
        first=False
    return(mu)

def cutter(coord):
    cStr=str(coord)
    cSplit=cStr.split(",",1)
    x=next(iter(cSplit))
    y=cSplit.pop()
    x=x.translate(str.maketrans("","",' ()'))
    y=y.translate(str.maketrans("","",' ()'))
    res=[]
    res.extend((float(x),float(y)))
    return res

def getMinDist4D(x,mu):
	minDist=999999999
	classMin=None
	i=0
	for point in mu.values():

		dist=math.sqrt( math.pow((point[0]-x[0]),2) + math.pow((point[1]-x[1]),2) \
			+ math.pow((point[2]-x[2]),2) + math.pow((point[3]-x[3]),2) )
		if dist<minDist:
			minDist=dist
			classMin=i
		i+=1
	return classMin


def getNextPoint(rows):
    row = next(rows)
    if(row and row.startall):
        s=cutter(row.startall)
        a=cutter(row.arrivalall)
        x=[]
        x.extend((s[0],s[1],a[0],a[1]))
        return x
    else:
    	return None

def getFirsts(rows,K):
    toSkip=randint(0, 100-K)
    for i in range(0,toSkip):
    	next(rows)
    mu={}
    for i in range(0,K):
        mu[i]=getNextPoint(rows)
    return mu



def plot(mu,name):
	mapit = folium.Map( location=[41.1579,-8.6291], zoom_start=11 )
	i=0
	for coord in mu.values():
		folium.Marker( location=[ coord[1], coord[0] ],popup='start '+str(i)).add_to( mapit )
		folium.Marker( location=[ coord[3], coord[2] ], icon = folium.Icon(color ='red'),popup='arrival '+str(i)).add_to( mapit )

		folium.PolyLine(
			[[coord[1], coord[0]],
			 [coord[3], coord[2]] ],color="red", weight=2.5, opacity=1
		).add_to(mapit)



		i+=1

	mapit.save( '{0}.html'.format(name))

mu=kmeansOffline(6,'hours<16')
plot(mu,'hoursInf16')
mu=kmeansOffline(6,'hours>16')
plot(mu,'hoursSup16')