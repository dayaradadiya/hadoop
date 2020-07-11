from pyspark import SparkConf, SparkContext

def LoadMovieNames():
	movienNmes = {}
	with open("/home/maria_dev/ml-100k/u.item") as f:
		for line in f:
			fields = line.split('|')
			movienNmes[int(fields[0])] = fields[1]
	return 	movienNmes		

#take each ine of data in line and convert it into (movieid,(rating,1.0))
#this way we can add up all the ratingfor each movie, and
#the total number of rating for each movie (which lets us compute the average)

def parseInput(line):
	fields = line.split()
	return (int(fields[1]),(float(fields[2]),1.0))

if __name__ =="__main__":
	
	#the main script to create spark context
	conf =  SparkConf().setAppName("Test").set("spark.driver.memory", "1g")
	sc = SparkContext(conf = conf)
	#Load up our movieid ->moviename lookup table
	
	movieNames = LoadMovieNames()
	
	#load up the raw u.data file
	lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
	
	#convert to (movieid,(rating,1.0)) 
	movieRatings = lines.map(parseInput)
	
	#reduce to (movieid,(sumofratings,totalratings))
	ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2 : (movie1[0] + movie2[0]))
	
	#Mapt to (movieid, averagerating)
	averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0]/totalAndCount[1])
	
	#sort by averagerating
	sortedMovies = averageRatings.sortBy(lambda x: x[1])
	
	#take the top 10 results
	results = sortedMovies.take(10) 
	
	#print them out
	for result in results:
		print(movienNmes[result[0]], result[1])
	
	