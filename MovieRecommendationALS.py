from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit
import numpy as nm

#load up movie ID -> movie name dictionary

def loadMovieNames():
    movieNames = {}
    with open("/home/maria_dev/ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii','ignore')
    return movieNames
    
 #convert udata line into (userid,movieid,rating) rows
 
def parseInput(line):
    fields = line.value.split()
    return Row(userID = int(fields[0]),movieID = int(fields[1]), rating = float(fields[2]))
    
    
if __name__ == "__main__":
    #create a sparksession (the config bi is only for windows)
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()
    
    #load movie id name dictionary
    movieNames = loadMovieNames()
    
    #get the raw data
    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd
    
    #convert it to a RDD with row object
    ratingsRDD = lines.map(parseInput)
    
    #convert rdd to dataframe and cache it
    ratings = spark.createDataFrame(ratingsRDD).cache()
    
    #create an als collaberative filtering model from the complete dataset
    als = ALS(maxIter=5, regParam=0.01, userCol = "userID", itemCol = "movieID", ratingCol = "rating" )
    model = als.fit(ratings)
    
    #print rating from user 0
    userRatings = ratings.filter("userID = 0")
    
    #print all
    #for rat in ratings.collect():
     #   print movieNames[rat["userID"]], rat["rating"]

    #pring top 20 recommendation
    #find movies rated more then 100 times
    
    ratingcount100 = ratings.groupby ("movieID").count().filter("count>100")
    
    #construct test dataframe for user 0 with every movie rated 100 times
    popularMovies = ratingcount100.select("movieID").withColumn('userID',lit(0) )
    
    #run model on that list of popular movies
    recommendations = model.transform(popularMovies)
    
    
    #get top 20 movies from this recommendation for this user
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)
    
    #print
    for recommendation in topRecommendations:
        print(movieNames(recommendation['movieID']),recommendation['prediction'])
        
    #stop spark session
    spark.stop()
    
    

    
    
    
    
    