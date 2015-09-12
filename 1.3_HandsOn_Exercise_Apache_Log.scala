// Databricks notebook source exported at Sat, 12 Sep 2015 19:29:48 UTC
// MAGIC %md
// MAGIC # Hands-on: Play with Apache Log

// COMMAND ----------

// MAGIC %md
// MAGIC #### Setup S3 Credential (Same for the whole course)

// COMMAND ----------

sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAJIWXMZ5GH7WJ5UDQ")
sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load file to RDD

// COMMAND ----------

val myLogRDD = sc.textFile("s3n://mlonspark/NASA_access_log_Aug95").
map(_.split(" (?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1))
.filter(_.length==8)
.map(line=>(line(0),line(5),line(6),line(7)))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Count total number of Lines

// COMMAND ----------

myLogRDD.count()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Print the first 10 lines of the log file

// COMMAND ----------

myLogRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Print the first 10 lines

// COMMAND ----------

myLogRDD.take(10)
.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Print the first 10 lines where the method is POST

// COMMAND ----------

val newList = List(1,2,3);
val checkOutput = newList.drop(2)

// COMMAND ----------

myLogRDD
.filter(line => (line._2.split(" ")(0).drop(1)=="POST"))
.take(10)
.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Count the number of DISTINCT Requester Domain Name 

// COMMAND ----------

myLogRDD
.map(line => (line._1,1))
.reduceByKey(_+_)
.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Find the top 10 Requester 

// COMMAND ----------

myLogRDD
.map(line => (line._1,1))
.reduceByKey(_+_)
.map(tpl => (tpl._2,tpl._1))
.sortByKey(false)
.take(10)
.foreach(println)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### Count the number of POST and GET log lines 

// COMMAND ----------

val postCount = myLogRDD.filter(line => line._2.split(" ")(0).drop(1)=="POST").count()
val getCount = myLogRDD.filter(line => line._2.split(" ")(0).drop(1)=="GET").count()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Another great approach to come to the same above results is the following

// COMMAND ----------

myLogRDD.map(line => line._2.split(" ")(0).drop(1))
.filter(method => (method=="POST" || method == "GET"))
.map((_,1))
.reduceByKey(_+_)
.take(10)

// COMMAND ----------

myLogRDD.map(line => line._2.split(" ")(0).drop(1)).first()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Find Distribution of Return Code

// COMMAND ----------

val retCodeCountRDD = myLogRDD.map(line => (line._3,1))
.reduceByKey(_+_)
.map(l => (l._2,l._1))
.sortByKey(false)

// COMMAND ----------

retCodeCountRDD.collect.foreach(println)

// COMMAND ----------

display(retCodeCountRDD.toDF("Counts","Return Codes"))

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC #### Challenge! Draw the above distribution as bar charts

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### Think about something else to play with the Log!

// COMMAND ----------

