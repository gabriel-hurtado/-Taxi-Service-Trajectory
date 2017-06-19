import os
import re
from decimal import Decimal
import collections
import itertools
import math
from cassandra.cluster import Cluster
import folium
from folium import plugins

cluster = Cluster(control_connection_timeout=9990.0)
session = cluster.connect()
session.default_timeout = 9999.0
session.set_keyspace("e39_taxi")

from math import radians, cos, sin, asin, sqrt
def haversine(lon1, lat1, lon2, lat2):
	"""
	Calculate the great circle distance between two points 
	on the earth (specified in decimal degrees)
	"""
	# convert decimal degrees to radians 
	lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
	# haversine formula 
	dlon = lon2 - lon1 
	dlat = lat2 - lat1 
	a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
	c = 2 * asin(sqrt(a)) 
	km = 6367 * c
	return km

#0		   1		  2			  3			 4			  5		6		   7		   8			 
#"TRIP_ID","CALL_TYPE","ORIGIN_CALL","ORIGIN_STAND","TAXI_ID","TIMESTAMP","DAY_TYPE","MISSING_DATA","POLYLINE"



Point = collections.namedtuple('Point', ['x', 'y'])

def cutter(coord):
	cStr=str(coord)
	cSplit=cStr.split(",",1)
	x=next(iter(cSplit))
	y=cSplit.pop()
	x=x.translate(str.maketrans("","",' ()'))
	y=y.translate(str.maketrans("","",' ()'))
	p = Point(x, y)
	return p

def convertFromStr(pt):
	x=float(pt.x)
	y=float(pt.y)
	p = Point(x, y)
	return p

def getMinDist(allComb):
	w=999999999
	for pair in allComb:
		first=pair[0].get("start")
		second=pair[1].get("start")
		dist=haversine(first.x,first.y,second.x,second.y)
		if dist<w:
			w=dist
	return w


def kMeansOStart(k):
	list_Classes=[]
	rows = session.execute('SELECT TripId, startall, arrivalall FROM startArrivalSorted ')#limit 1000')
	nb_Class=0;
	for row in rows:
		#Return a point with values x and y
		s=cutter(row.startall)
		#a=cutter(row.arrival)
		s=convertFromStr(s)
		if(nb_Class<k+1): #for the k+1 first
			nb_Class=nb_Class+1
			list_Classes.append({"id":row.tripid,"start":s,"class":nb_Class})
			#get w
			if(nb_Class==k+1): #init the params   
				allComb=list(itertools.combinations(list_Classes, 2)) 		
				dist=getMinDist(allComb)
				w=20000*dist #dist/2
				f=w/k
				r=1
				q=0			
				n=k+1
		else:
			allComb=[]
			n=n+1
			for point in list_Classes:
				allComb.append([point,{"id":row.tripid,"start":s,"class":0}])
			#print(allComb)
			dist=getMinDist(allComb)
			#print(dist/f)
			if(dist/f>1):
				nb_Class=nb_Class+1
				list_Classes.append({"id":row.tripid,"start":s,"class":nb_Class})
				q=q+1
				print("added")
			if(q>= 3*k*(1 + math.log(n))):
				r=r+1
				q=0
				f=2*f
				print("f=",f)
	return list_Classes


def getMinDist4D(allComb):
	w=999999999
	for pair in allComb:
		firstS=pair[0].get("start")
		secondS=pair[1].get("start")
		firstA=pair[0].get("arrival")
		secondA=pair[1].get("arrival")		
		dist=math.sqrt( math.pow((firstS.x-secondS.x),2) + math.pow((firstS.y-secondS.y),2) + math.pow((firstA.x-secondA.x),2) + math.pow((firstA.y-secondA.y),2) )
		if dist<w:
			w=dist
	return w

def kMeansOStartArrival(k):
	i=0
	list_Classes=[]
	rows = session.execute('SELECT TripId, startall, arrivalall FROM startArrivalSorted ')#limit 100000')
	nb_Class=0;
	for row in rows:
		i+=1
		#Return a point with values x and y
		#print(row)
		s=cutter(row.startall)
		a=cutter(row.arrivalall)
		s=convertFromStr(s)
		a=convertFromStr(a)
		if(nb_Class<k+1): #for the k+1 first
			nb_Class=nb_Class+1
			list_Classes.append({"id":row.tripid,"start":s,"arrival":a,"class":nb_Class})
			#get w
			if(nb_Class==k+1): #init the params   
				allComb=list(itertools.combinations(list_Classes, 2)) 		
				dist=getMinDist4D(allComb)
				#print(dist)
				w=dist #dist/2
				f=w/k
				r=1
				q=0			
				n=k+1
		else:
			allComb=[]
			n=n+1
			for point in list_Classes:
				allComb.append([point,{"id":row.tripid,"start":s,"arrival":a,"class":0}])
			#print(allComb)
			dist=getMinDist4D(allComb)
			#print(dist/f)
			if(dist/f>1 and dist<(f+(f/500))): #dist<(f+(f/500))to remove outliers
				nb_Class=nb_Class+1
				list_Classes.append({"id":row.tripid,"start":s,"arrival":a,"class":nb_Class})
				q=q+1
				print(dist)
				print({"id":row.tripid,"start":s,"arrival":a,"class":nb_Class})
			if(q>= 0.5*k*(1 + math.log(n))):#3*k*(1 + math.log(n))):
				r=r+1
				q=0
				f=2*f
				print('upp')
				#print("f=",f)
	print(i)
	return list_Classes

def convertPlotting(mu):
    muNew={}
    for i in range(0,(len(mu)-1)):
        muNew[i]=[  mu[i].get("start").x, mu[i].get("start").y, mu[i].get("arrival").x, mu[i].get("arrival").y]
    return muNew

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

mu=kMeansOStartArrival(1)
mu=convertPlotting(mu)
plot(mu,'onlinek_homeTuned')
	   
