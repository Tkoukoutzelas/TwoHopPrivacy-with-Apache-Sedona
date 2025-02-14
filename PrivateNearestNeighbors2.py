#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 11:45:19 2017

@author: alexandros
"""
"""
Throughout this code first there is x coordinate and then y.
"""
import random as rnd
import numpy as np
#import matplotlib.pyplot as plt
from math import radians, cos, sin, asin, sqrt
from datetime import datetime
from sedona.core.geom.envelope import Envelope
from sedona.core.spatialOperator import RangeQuery
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

def getDataFromDb(limit=0):
    try:
        #connect_str = "dbname='pgsnapshot' user='postgres'"

        connect_str = "dbname='pgsnapshot' user='postgres'"
        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)
        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()
        # run a SELECT statement - no data in there, but we can try it
        statement = """SELECT ST_Y(geom), ST_X(geom) FROM nodes """
        
        if limit>0:
            statement += """ORDER BY RANDOM() LIMIT """+str(limit)
#        cursor.execute("""SELECT ST_X(geom), ST_Y(geom) FROM nodes LIMIT 10""")
        cursor.execute(statement)
        rows = cursor.fetchall()
        """
        if limit>0:
            print(rows)
        else:
            print(len(rows))
        """
        return rows
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)
        
        
def createUsers(data, num_users):
    rnd1 = rnd
    rnd1.seed(datetime.now())
    users=[]
    users_inverted_index={}
    for _ in range(0,num_users):
        index = rnd1.randrange(len(data))
        #print(index, end=' ')
        users.append(data[index])
        users_inverted_index[data[index]]=index
    #print()
    return np.array(users), users_inverted_index

def calcNearest(p, index, num):
    nearest=list(index.nearest((p[0], p[1], p[0], p[1]), num, objects="raw"))
    return nearest

def calculateAllNearest(p_list, index, num):
    allpoints=[]
    for p in p_list:
        allpoints.append(list(calcNearest(p, index, num)))
    return allpoints

def getValidIPOI(user, poisList):
    index=0
#    print(user, poisList[index])
#    _ = input("getValidIPOI 1:")
    while tuple(user)==poisList[index]:
        index +=1
#        print(user, poisList[index])
#        _ = input("getValidIPOI 2:")
#    print(user, poisList[index])
#    _ = input("getValidIPOI 3:")
    return poisList[index]
        
# Calculate IPOIs list
def calculateIPOis(users, index):
    cand_iPOIS = calculateAllNearest(users, index, 50)
    iPois = []
 #   print(users, cand_iPOIS)
    for user, sublist in enumerate(cand_iPOIS):
        iPois.append(getValidIPOI(users[user],sublist))
    return np.array(iPois)

#def populateIndex(data, index):
#    for pointid, point in enumerate(data):
#        index.insert(pointid, (point[0], point[1], point[0], point[1]))        
#    return(index)

# Fill R*-tree
def generator_function(data):
    for i, obj in enumerate(data):
        yield (i, (obj[0], obj[1], obj[0], obj[1]), obj)
        #print(obj)

# Calculate sides of the square and distances
#def calculateIpoiIDDistances(users, poiids, data):
#    distances = [geod.Inverse(users[i][0], users[i][1], 
#                data[poiids[i]][0], data[poiids[i]][1])['s12'] 
#                for i,_ in enumerate(users)]
#    return distances



def calculateIpoiDistances(users, pois, data):
    distances = [np.linalg.norm(users[i]-pois[i]) for i,_ in enumerate(users)]
    return distances

#def new_point(y0, x0, d, theta):
#    theta_rad = pi/2 - radians(theta)
#    return y0 + d*sin(theta_rad), x0 + d*cos(theta_rad)

#Created for Sedona Implementation
def new_point_x(x0, d, angle):
    x = d * cos(radians(angle));
    return x0+x
#Created for Sedona Implementation   
def new_point_y(y0,d,angle):
    y = d * sin(radians(angle));
    return y0+y    

# >>> left, bottom, right, top = (0.0, 0.0, 1.0, 1.0)
# Azimuths are from north clockwise. 
# So, bottom left is SW with azimuth = 225 degrees
# and, top right is NE with azimuth = 45 degrees
def calcBoundingSquare(side,x,y):
	#with open('/home/bigdata/Desktop/sedona/testfile.txt') as f:
		#print(new_point(ipoi[0], ipoi[1], side, 225))
	return new_point(y,x, side, 225)+new_point(y,x, side, 45)

def calcBoundingSquares(sides, ipois):
    """
    Calculates the bounding square given a side size and ipois.
    """        
    aa = [ calcBoundingSquare(sides[i],ipois[i]) for i,s in enumerate(sides)]
    
#        aa = [new_point(ipois[i][0], ipois[i][1], sides[i], 225)+
#              new_point(ipois[i][0], ipois[i][1], sides[i], 45) 
#              for i,_ in enumerate(sides)]

#    for a in aa:
#            print(a)
    return(aa)
    #return tuple(new_point(ipois[0][0], ipois[0][1], sides[0], 225)+
    #             new_point(ipois[0][0], ipois[0][1], sides[0], 45))
    
def rangeQuery(queryRectangle, index):
    return(list(index.intersection(queryRectangle, objects='raw')))
  
def rangeQueryCount(queryRectangle, index):
    print(queryRectangle)
    envelop = Envelope(queryRectangle[0], queryRectangle[1], queryRectangle[2], queryRectangle[3])
    #envelop = Envelope([-90.01,-80.01], [30.01, 40.01])
    #envelop = Envelope.from_shapely_geom(queryRectangle)
    print(envelop)
    num_of_elements = RangeQuery.SpatialRangeQuery(index, envelop, False, False)
    print(num_of_elements.count())
    return num_of_elements.count()


def allRangeQueries(queryRectangles, index):
    return([rangeQuery(qr, index) for qr in queryRectangles])
   
def kAnonymousAllRangeQueries(queryRectangles, index, sides, ipois, k):
    #for i, _ in enumerate(queryRectangles):
    num_of_elements = rangeQueryCount(queryRectangles[i], index)
      #print("For poi no:"+str(i)+" elements_found:"+str(num_of_elements))
    while num_of_elements < k:
         sides[i] = sqrt(2)*sides[i] # Double square's area
         queryRectangles[i] = calcBoundingSquare(sides[i],ipois[i])
         #print(str(i)+" Rect:"+str(queryRectangles[i])+" Side:"+str(sides[i])+" Ipoi:"+str(ipois[i]))
         #input("press enter:")
         num_of_elements = rangeQueryCount(queryRectangles[i], index)
         #print("For poi no:"+str(i)+" elements_found:"+str(num_of_elements))
    return([rangeQuery(qr, index) for qr in queryRectangles])
    #return queryRectangles


def selectSPOI(cand_SPOIs):
   rnd2 = rnd
   rnd2.seed(datetime.now())
   spois = []
   for cspoi in cand_SPOIs:
       selection = rnd2.randrange(len(cspoi))
       #print("=======>"+str(selection)+" "+str(len(cspoi)))
       spois.append( cspoi[selection] )
   return spois
   
def findAllNearestNeighbors(points, K, index):
    nearest_neigbors=[ list(index.nearest(( p[0], p[1], p[0], p[1]), K)) for p in points]
    return nearest_neigbors

def buildSPOIsInvertedIndex(spois):
    """
    SPOIs inverted index
    """
    invSpoisIndex = {}
    for ind,spoi in enumerate(spois):
        invSpoisIndex[ind]=spoi
    return(invSpoisIndex)



def plotting(users, ipois,cand_SPOIs, SPOIs,SMin,SMax):


    #pi = plt.scatter(*zip(*ipois), marker="o", color="green")

    pf=[]
    for o in cand_SPOIs:
        pf.append(
                plt.scatter(
                        *zip(o), 
                        marker="*", facecolors='none', edgecolors="black"))
       # print(o)

    sf=[]
    #for i,s in enumerate(SPOIs):
        #print(s)
    #    sf.append( plt.scatter(*zip(s), marker="+", color="red"))
    #    sf.append( plt.annotate(i, s))
    """   
    p1=[]
    p2=[]
    for i,_ in enumerate(sides):
        p1.append(
                plt.scatter(
                        *zip(new_point(ipois[i][0], ipois[i][1], sides[i], 225)), 
        marker="^", color="red"))
        p2.append( 
                plt.scatter(
                        *zip(new_point(ipois[i][0], ipois[i][1], sides[i], 45)),
                        marker="v", color="red")) 
    
        p1.append(plt.annotate(i, new_point(ipois[i][0], ipois[i][1], sides[i], 225)))
        p1.append(plt.annotate(i, new_point(ipois[i][0], ipois[i][1], sides[i], 45)))
    """   
    #po = plt.scatter(*zip(*users), marker="X", color="b")
    #for i,u in enumerate(users):
    #	plt.annotate(i,u)
    smi=[]	
    for i,s in enumerate(SMin):
    	smi.append(plt.scatter(*zip(s),marker="v", color="red"))
    	smi.append( plt.annotate(i, s))
    	
    sma=[]
    for i,s in enumerate(SMax):
    	sma.append(plt.scatter(*zip(s),marker="^", color="red"))
    	sma.append( plt.annotate(i, s))    	
    	
    	 	
    plt.legend((pf, *sf),
           ('user', 'ipoi', 'spoi'),
           scatterpoints=1,
           loc='lower left',
           ncol=5,
           fontsize=8)

    plt.show()
    
    
    
def compareWithGroundTruth(method, ground, users, K):
    """
    Ground truth number of results: users*K
    Precision = count/K*users
    Recall = count/sum
    """
    count=0
    for index, m  in enumerate(method):
        for m_elem in m:
            if m_elem in ground[index]:
                count += 1
    ground_truth = sum([len(g) for g in ground])
    return count, ground_truth, K*users, count/(K*users), count/ground_truth
  
  
  
  
  
  
  
  
  
            
"""       
def makeBoundingSquares(bound,sides,sparkSession):
	a = [0,0,0,0]
	a[0] = float(bound[0])
	a[1] = float(bound[1])
	a[2] = float(bound[2])
	a[3] = float(bound[3])
	testdf5=sparkSession.sql("SELECT geom FROM nodes_2 WHERE ST_Contains(ST_PolygonFromEnvelope("+str(a[0])+", "+str(a[1])+","+ str(a[2]) + "," + str(a[3])+"), geom)")
	print("Count:" + str(testdf5.count()))
	j = 0
	while testdf5.count() < 10 and j < 10:
		a[0]=a[0]-0.0001
		a[1]=a[1]+0.0001
		a[2]=a[2]+0.0001
		a[3]=a[3]-0.0001
		print(a)
		testdf5=sparkSession.sql("SELECT geom FROM nodes_2 WHERE ST_Contains(ST_PolygonFromEnvelope("+str(a[0])+", "+str(a[1])+","+ str(a[2]) + "," + str(a[3])+"), geom)")
		print("Iteration:" + str(j) + "Count:" + str(testdf5.count()))
		j=j+1
	testdf5.show()
	testdf5.createOrReplaceTempView("bound")
"""	                   
