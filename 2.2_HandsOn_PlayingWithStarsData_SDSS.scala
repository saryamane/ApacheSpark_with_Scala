// Databricks notebook source exported at Sat, 12 Sep 2015 19:35:43 UTC
// MAGIC %md
// MAGIC # Play with Stars observed by a real Telescope
// MAGIC Note: real astronomy may do similar things

// COMMAND ----------

// MAGIC %md
// MAGIC #### Setup S3 Credential and Load File

// COMMAND ----------

sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAJIWXMZ5GH7WJ5UDQ")
sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")
val myRDD = sc.textFile("s3n://mlonspark/sdss.txt")


// COMMAND ----------

myRDD.first()
//The data look like the following:
// 69068950,308.119646267,35.4419336308,19.7586,18.2142
// 71078341,349.28343638,54.5848277797,18.4068,17.6605
// 71450647,355.444119504,43.6987423493,21.6972,20.3539

// It is a CSV file, each line have 5 values, they are (with type):
// ID    ,      X,      Y, Magnitude, SomeThingElse
// String, double, double, double   , double

// COMMAND ----------

// MAGIC %md
// MAGIC #### Turn the input data into an RDD of 5-tuples. 
// MAGIC Each Tuple should be like (ID, X, Y, Magnitude, SomethingElse). The values should be in their proper type

// COMMAND ----------

val tuples=myRDD.map(_.split(","))
.map(line=>(line(0),line(1).toDouble,line(2).toDouble, line(3).toDouble,line(4).toDouble))

tuples.take(3).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Draw where all the stars are in a 2D map
// MAGIC Hint: convert to DF

// COMMAND ----------

val df = tuples.toDF("ID","X","Y","Magnitude","SomethingElse")
display(df)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC #### Find min/max value of magnitude

// COMMAND ----------

df.describe("Magnitude").collect()

// COMMAND ----------

val mmin = tuples.map(_._4).min()
val mmax = tuples.map(_._4).max()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Use O(n) time to get the brightest Star (i.e. smallest Magnitude), and return all info about that star
// MAGIC Hint: Use reduce

// COMMAND ----------

tuples.reduce((t1,t2) => if(t1._4<t2._4) t1 else t2)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC #### Draw a histogram of Magnitude, bin to Integers from 12 to 30
// MAGIC i.e. if a star's magnitude is 12.65, put it into the bin 12. 

// COMMAND ----------

val df = tuples.map(t =>(t._4.toInt,1))
.reduceByKey(_+_)
.sortByKey()
.toDF("Magnitude","Count")
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Find the pair of stars that are the farthest away from each other
// MAGIC (Assume the distance between two stars is sqrt((X1-X2)^2+(Y1-Y2)^2))
// MAGIC 
// MAGIC Hint, use cartisian to make a "self-crossjoin"

// COMMAND ----------

val ct = tuples
.map(t =>(t._1, t._2, t._3))
.cartesian(tuples.map(t => (t._1, t._2, t._3))
)
.map(s => (s._1._1, s._2._1,math.sqrt((s._1._2-s._2._2)*(s._1._2-s._2._2)+(s._1._3-s._2._3)*(s._1._3-s._2._3))))
.reduce((s1,s2) => (if(s1._3 > s2._3) s1 else s2))

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Find the star with the most number of neighbors within radius R. 
// MAGIC I.e. For each star, draw a circle of radius R around it, then count how many stars are in the circle, then do a max. 
// MAGIC 
// MAGIC Result is: Star ID=168138483 has 15 neighbours within Radius 10

// COMMAND ----------

val Radius=10.0
val mm = tuples
.map(t =>(t._1, t._2, t._3))
.cartesian(tuples.map(t => (t._1,t._2,t._3))
)
.map(
  s => (
  s._1._1,
    if(math.sqrt((s._1._2-s._2._2)*(s._1._2-s._2._2) + (s._1._3 - s._2._3)*(s._1._3 - s._2._3)) < Radius) 1 else 0
  )
)
.reduceByKey(_ + _)
.map(s=>(s._2,s._1))
.sortByKey(false)
.take(10)

// COMMAND ----------

