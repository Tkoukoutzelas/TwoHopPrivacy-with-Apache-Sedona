from pyspark.sql.functions import rand
from sedona.spark import SedonaContext
from sedona.core.enums import IndexType
import PrivateNearestNeighbors2 as pnn
from pyspark import SparkContext
from sedona.spark import *
from pyspark.sql.types import FloatType,DoubleType
from pyspark.sql.functions import col,udf,struct,lit
from math import sqrt
from pyspark.sql import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import broadcast
import time
import sys

def expandBounding(userid,dist,min_x,min_y,max_x,max_y,originalx,originaly):
	distNew=dist*sqrt(2);
	minx=pnn.new_point_x(originalx,distNew,225)
	miny=pnn.new_point_y(originaly,distNew,225)
	maxx=pnn.new_point_x(originalx,distNew,45)
	maxy=pnn.new_point_y(originaly,distNew,45)
	return userid,distNew,minx,miny,maxx,maxy,originalx,originaly



u,k,p,parts,nn=int(sys.argv[1]),int(sys.argv[2]),int(sys.argv[3]),int(sys.argv[4]),int(sys.argv[5])

# Create a SparkSession
config = SedonaContext.builder() .\
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-shaded-3.3_2.12:1.7.0,'
           'org.datasyslab:geotools-wrapper:1.7.0-28.5'). \
    getOrCreate()
sedona = SedonaContext.create(config)
 
#Set configurations for execution 
sedona.conf.set("spark.sql.adaptive.enabled", "true")
sedona.conf.set("sedona.global.index","true")
sedona.conf.set("sedona.global.indextype","rtree")
sedona.conf.set("sedona.join.optimizationmode","all")
sedona.conf.set("sedona.join.autoBroadcastJoinThreshold","200MB")
sedona.conf.set("spark.default.parallelism",parts)
sedona.conf.set("spark.sql.shuffle.partitions",parts)
sedona.conf.set("spark.sql.files.minPartitionNum",parts)



start_time = time.time()

#Set a checkpoint in HDFS
sedona.sparkContext.setCheckpointDir("hdfs://snf-15511.ok-kno.grnetcloud.net:9000/user/themis")

#Loading the dataset and selecting the pois and the users
dataset = sedona.read.options(header='True', inferSchema='True', delimiter=',').format("csv").load("hdfs://snf-15511.ok-kno.grnetcloud.net:9000/user/themis/nodes.csv.gz").repartition(parts)
dataset.createOrReplaceTempView("nodes_1")
pois=sedona.sql("SELECT id,ST_FlipCoordinates(st_geomFromWKT(geom)) as geom from nodes_1 WHERE id<="+str(p)+" ORDER BY id").repartition(parts).cache()
pois.createOrReplaceTempView("pois")
users = sedona.sql("SELECT id, geom from pois WHERE id<="+str(u)+" ORDER BY id")
users.createOrReplaceTempView("users")
users=broadcast(users)

#Finding the IPOIs
nearest_points = sedona.sql("SELECT users.id as userid,users.geom as usergeom,pois.id as poiId,pois.geom as poisgeom,ST_DISTANCE(users.geom,pois.geom) as dist FROM users JOIN pois ON ST_KNN(users.geom,pois.geom,2,FALSE) ORDER BY userid").repartition(parts).cache()
nearest_points.createOrReplaceTempView("nearestDupl")
real_nearest = sedona.sql("SELECT * FROM nearestDupl WHERE dist !=0.0 ORDER BY userid")
real_nearest.createOrReplaceTempView("sides_ipois")

#Forming the first Bounding Squares
extract_coords=sedona.sql("SELECT *,ST_X(poisgeom) as xcord,ST_Y(poisgeom) as ycord FROM sides_ipois").repartition(parts)
extract_coords.createOrReplaceTempView("extract_coords")
newPointXUDF=udf(lambda x,y,d:pnn.new_point_x(x,y,d),DoubleType())
newPointYUDF=udf(lambda x,y,d:pnn.new_point_y(x,y,d),DoubleType())
boundingCoords=extract_coords.withColumn("minx",newPointXUDF(col("xcord"),col("dist"),lit(225)))
boundingCoords2=boundingCoords.withColumn("miny",newPointYUDF(col("ycord"),col("dist"),lit(225)))
boundingCoords3=boundingCoords2.withColumn("maxX",newPointXUDF(col("xcord"),col("dist"),lit(45)))
boundingCoordsFinal=boundingCoords3.withColumn("maxy",newPointYUDF(col("ycord"),col("dist"),lit(45))).repartition(parts)
boundingCoordsFinal.createOrReplaceTempView("coords_ids")
boundingSquares=sedona.sql("SELECT userid,xcord,ycord,ST_PolygonFromEnvelope(minx,miny,maxX,maxY) as boundSquares,dist,minx,miny,maxX,maxY FROM coords_ids ")
boundingSquares.createOrReplaceTempView("bounds")
boundingSquares =broadcast(boundingSquares)

#Identifying which squares satisfy k-anonymity and which do not
kAnonimitySatisfied=sedona.sql("SELECT COUNT(*),bounds.userid,bounds.dist,bounds.minx,bounds.miny,bounds.maxX,bounds.maxY,bounds.xcord,bounds.ycord FROM bounds,pois WHERE ST_Contains(bounds.boundSquares,pois.geom) GROUP BY bounds.dist,bounds.minx,bounds.miny,bounds.maxX,bounds.maxY,bounds.userid,bounds.xcord,bounds.ycord HAVING COUNT(*)>="+str(k)).repartition(parts)
kAnonimityNotSatisfied=sedona.sql("SELECT COUNT(*),bounds.userid,bounds.dist,bounds.minx,bounds.miny,bounds.maxX,bounds.maxY,bounds.xcord,bounds.ycord FROM bounds,pois WHERE ST_Contains(bounds.boundSquares,pois.geom) GROUP BY bounds.dist,bounds.minx,bounds.miny,bounds.maxX,bounds.maxY,bounds.userid,bounds.xcord,bounds.ycord  HAVING COUNT(*)<"+str(k)).repartition(parts).cache()


columnsRename=["userid","dist","minx","miny","maxX","maxY","xcord","ycord"]


#Iterations to form the rest of the Bounding Squares
while(True):
	poisCount=kAnonimityNotSatisfied.count()
	print(poisCount)
	if(poisCount==0):
		print("All squares satisfy k-anonymity")
		break
	else:
		newCoordsAndDist=kAnonimityNotSatisfied.rdd.map(lambda x:expandBounding(x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]))
		newCoordsAndDistDF=sedona.createDataFrame(newCoordsAndDist,columnsRename).repartition(parts)
		newCoordsAndDistDF.createOrReplaceTempView("newCoordsAndDist")
		newSquares=sedona.sql("SELECT userid,xcord,ycord,ST_PolygonFromEnvelope(minx,miny,maxX,maxY) as boundSquares,dist,minx,miny,maxX,maxY FROM newCoordsAndDist").repartition(parts)
		newSquares.createOrReplaceTempView("newSquares")
		newSquares =broadcast(newSquares)
		kAnonimitySatisfiedLoop=sedona.sql("SELECT COUNT(*),newSquares.userid,newSquares.dist,newSquares.minx,newSquares.miny,newSquares.maxX,newSquares.maxY,newSquares.xcord,newSquares.ycord FROM newSquares,pois WHERE ST_Contains(newSquares.boundSquares,pois.geom) GROUP BY newSquares.dist,newSquares.minx,newSquares.miny,newSquares.maxX,newSquares.maxY,newSquares.userid,newSquares.xcord,newSquares.ycord HAVING COUNT(*)>="+str(k)).repartition(parts).cache()
		kAnonimitySatisfied=kAnonimitySatisfied.union(kAnonimitySatisfiedLoop)
		kAnonimityNotSatisfied=(sedona.sql("SELECT COUNT(*),newSquares.userid,newSquares.dist,newSquares.minx,newSquares.miny,newSquares.maxX,newSquares.maxY,newSquares.xcord,newSquares.ycord FROM newSquares,pois WHERE ST_Contains(newSquares.boundSquares,pois.geom) GROUP BY newSquares.dist,newSquares.minx,newSquares.miny,newSquares.maxX,newSquares.maxY,newSquares.userid,newSquares.xcord,newSquares.ycord HAVING COUNT(*)<"+str(k)).repartition(parts).checkpoint())


#Forming the final bounding Squares
kAnonimitySatisfied.createOrReplaceTempView("finalCoords")
kAnonimitySatisfied=sedona.sql("SELECT * FROM finalCoords ORDER BY userid ").repartition(parts).cache()
finalSquares=sedona.sql("SELECT userid,ST_PolygonFromEnvelope(minx,miny,maxX,maxY) as boundSquares FROM finalCoords").repartition(parts).cache()
finalSquares.createOrReplaceTempView("finalSquares")
finalSquares = broadcast(finalSquares)
squaresAndPoints=sedona.sql("SELECT * FROM finalSquares,pois WHERE ST_CONTAINS(finalSquares.boundSquares,pois.geom)").repartition(parts).cache()
squaresAndPoints.createOrReplaceTempView("squaresP")

#Selecting the SPOIs
spois=sedona.sql("WITH ranked_rows AS (SELECT *,ROW_NUMBER() OVER (PARTITION BY userid ORDER BY RAND(5)) AS rn FROM squaresP)SELECT * FROM ranked_rows WHERE rn = 1;").repartition(parts).cache()
spois.createOrReplaceTempView("spois")

#Finding the method's nearest neighbors
nearest_df = sedona.sql("SELECT a.userid AS user_id, b.userid AS nearest_neighbor_id, ST_Distance(a.geom, b.geom) AS distance FROM spois a JOIN spois b ON ST_KNN(a.geom, b.geom," + str(nn) + ", FALSE)").repartition(parts).cache()



nearest_df.show()
end_time = time.time()
elapsed= end_time-start_time
print("elapsed time:"+str(elapsed))
print(u,k,p,parts,nn)

#Finding the ground truth neighbors
ground_df = sedona.sql("SELECT a.id AS user_id, b.id AS nearest_neighbor_id, ST_Distance(a.geom, b.geom) AS distance FROM users a JOIN users b ON ST_KNN(a.geom, b.geom," + str(nn) + ", FALSE)")
ground_df.show()



ground_df.createOrReplaceTempView("ground")
nearest_df.createOrReplaceTempView("method")

#Calculate the precision
all_neighbors = u*nn
precision = sedona.sql("SELECT CAST(COUNT(*)/"+str(all_neighbors)+" AS FLOAT) FROM ground INNER JOIN method ON ground.user_id = method.user_id AND ground.nearest_neighbor_id = method.nearest_neighbor_id")
precision.show()