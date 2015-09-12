// Databricks notebook source exported at Sat, 12 Sep 2015 19:28:48 UTC
// MAGIC %md
// MAGIC # **Spark Basics in Scala** 

// COMMAND ----------

// MAGIC %md 
// MAGIC #### ** Hey! Does this work? **

// COMMAND ----------

1 + 1

// COMMAND ----------

// MAGIC %md
// MAGIC #### **Show the Spark Context**

// COMMAND ----------

sc

// COMMAND ----------

// MAGIC %md
// MAGIC #### Some Basic Map/Reduce Operations

// COMMAND ----------

// filter, count, reduce
// define a function to map
val numbers = sc.parallelize(1 to 1000)
.map(i => if(i >10) i else null)
.take(11)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Calculate Pi

// COMMAND ----------

// MAGIC %md
// MAGIC #### My own Pi example first

// COMMAND ----------

val NUM_SAMPLES = 200000;
val count = sc.parallelize(1 to NUM_SAMPLES)
.map{i => val x = Math.random()
val y = Math.random()
if ((x * x + y * y) < 1) 1 else 0} // This will return an array of Ints i.e. Array[Int]
.reduce(_+_) // Note: This is not reduceByKey function, you can always use that when you have a key value.
println("Pi is roughly " + 4.0 * count / NUM_SAMPLES)


// COMMAND ----------

// Test example to understand the reduce stage better.

val tuple = sc.parallelize(Array(("Samir",2),("Amit",1),("Samir", 5),("Test",5)))
tuple.reduceByKey(_+_).collect()

// COMMAND ----------

//need to define a function to map
val NUM_SAMPLES=100000;
val count = sc.parallelize(1 to NUM_SAMPLES).map{i =>
  val x = Math.random()
  val y = Math.random()
  if (x*x + y*y < 1) 1 else 0
}.reduce(_ + _)
println("Pi is roughly " + 4.0 * count / NUM_SAMPLES)


// COMMAND ----------

//need to define a function to map
val NUM_SAMPLES=100000;
val count = sc.parallelize(1 to NUM_SAMPLES).map{i =>
  val x = Math.random()
  val y = Math.random()
  if (x*x + y*y < 1) 1 else 0
}.sum()
println("Pi is roughly " + 4.0 * count / NUM_SAMPLES)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Setup S3 Credential (Same for the whole course)

// COMMAND ----------

sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAJIWXMZ5GH7WJ5UDQ")
sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")


// COMMAND ----------

// MAGIC %md
// MAGIC #### Open a File on S3 (1 Million Lines of Wikipedia)

// COMMAND ----------

val samirRDD = sc.textFile("s3n://mlonspark/wikipedia1Mlines.txt")

// COMMAND ----------

samirRDD.take(10)

// COMMAND ----------

samirRDD.first()

// COMMAND ----------

samirRDD.count()

// COMMAND ----------

// MAGIC %md

// COMMAND ----------

// MAGIC %md
// MAGIC ### It tokenizes every single word using the flatMap function

// COMMAND ----------

samirRDD.flatMap(_.split(" ")).count()

// COMMAND ----------

samirRDD.flatMap(_.split(" ")).take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Now lets get the word count example in Spark using Scala syntax.

// COMMAND ----------

val wordcount = samirRDD.flatMap(_.split(" "))
.map((_,1))
.reduceByKey(_+_)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Another way of doing this would be a more lenghtier code, but more understandable as well.

// COMMAND ----------

// MAGIC %md
// MAGIC #### The below code also gives you the count of distinct words in the text, as you are grouping or reducing by the key value

// COMMAND ----------

val wordcount = samirRDD.flatMap(line => line.split(" "))
.map(word => (word,1))
.reduceByKey((a,b) => a + b)
.count()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Now lets count the top occuring words which are of length >=6

// COMMAND ----------

samirRDD.flatMap(line => line.split(" "))
.filter(bigWord => bigWord.length() >= 6)
.map(word => (word,1))
.reduceByKey((a,b) => a + b)
.map(tpl => (tpl._2,tpl._1))
.sortByKey(false)
.take(10)

// COMMAND ----------

val myRDD = sc.textFile("s3n://mlonspark/wikipedia1Mlines.txt")

// COMMAND ----------

myRDD.take(10)

// COMMAND ----------

myRDD.count()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Count Total Number of Words

// COMMAND ----------

myRDD.flatMap(_.split(" ")).count()

// COMMAND ----------

myRDD.flatMap(_.split(" ")).take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Word Count?

// COMMAND ----------

myRDD.flatMap(_.split(" "))
.map(w =>(w,1))
.reduceByKey(_+_)
.take(10)

// COMMAND ----------

myRDD.flatMap(_.split(" "))
.map(w =>(w,1))
.reduceByKey((a,b) => a+b)
.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Count Total Number of UNIQUE Words

// COMMAND ----------

myRDD.flatMap(_.split(" "))
.map(w =>(w,1))
.reduceByKey((a,b) => a+b)
.count()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Find the most used 10 words

// COMMAND ----------

myRDD.flatMap(_.split(" "))
.map(w =>(w,1))
.reduceByKey((a,b) => a+b)
.map(t =>(t._2, t._1))
.sortByKey(false)
.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Find the 10 most used words with length >= 6

// COMMAND ----------

myRDD.flatMap(_.split(" "))
.filter(_.length() >= 6)
.map((_,1))
.reduceByKey(_+_)
.map(tpl => (tpl._2,tpl._1))
.sortByKey(false)
.take(10)

// COMMAND ----------

