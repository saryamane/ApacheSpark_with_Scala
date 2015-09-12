// Databricks notebook source exported at Sat, 12 Sep 2015 19:26:56 UTC
// MAGIC %md
// MAGIC # Collaborative Filtering on 100K MovieLens Dataset

// COMMAND ----------

// MAGIC %md
// MAGIC #### Load data from S3 and Create RDDs

// COMMAND ----------

sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "<Access Key>")
sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "<Private Key>")
val ratingRDD = sc.textFile("s3n://mlonspark/ml-100k/u.data")
val itemRDD = sc.textFile("s3n://mlonspark/ml-100k/u.item")
val userRDD = sc.textFile("s3n://mlonspark/ml-100k/u.user")
val readmeRDD = sc.textFile("s3n://mlonspark/ml-100k/README")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Look at the README file to see what fields are in each file

// COMMAND ----------

readmeRDD.collect.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Import proper libraries

// COMMAND ----------

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

// COMMAND ----------

// MAGIC %md
// MAGIC #### Train the model using ALS
// MAGIC Use rank=20, #iterations=15, lambda=0.01
// MAGIC 
// MAGIC Example Here:
// MAGIC http://spark.apache.org/docs/latest/mllib-collaborative-filtering.html

// COMMAND ----------

val rating = ratingRDD.map(_.split('\t') match { case Array(user, item, rate) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
  })
// Build the recommendation model using ALS
val rank = 20
val numIterations = 15
val model = ALS.train(ratings, rank, numIterations, 0.01)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Predict the rating of user 196 on item 557

// COMMAND ----------

// Evaluate the model on rating data
val user = 196
val product = 557
val usersProducts = ratings.map { case Rating(user, product, rate) =>
  (user, product)
}

val predictions = 
  model.predict(usersProducts).map { case Rating(user, product, rate) => 
    ((user, product), rate)
  }

// COMMAND ----------

// MAGIC %md
// MAGIC #### Calculate Mean Square Error
// MAGIC Hint: Copy/paste from the example in the link above :-)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC #### Now suggest top 5 items to user 196 (and print the movie names as well)
// MAGIC Hint: rate all items, join with the itemRDD, and return the highest rating 5 (note that don't return the ones the user already rated!)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC #### MSE vs. Iterations
// MAGIC 
// MAGIC Use Number of iterations=1,2,3,5,7,9,11, retrain the model, and plot the number of iterations vs. MSE (Shouldn't take more than 30 seconds to run)
// MAGIC I.e. Observe the MSE going down when the #of iterations go up
// MAGIC 
// MAGIC Also change the k and lambda numbers to observe the difference

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC #### Suggest some movies for yourself and see if it really works!
// MAGIC Hints:
// MAGIC 1. look into the itemRDD, find some movies that you watch before, and rate them!
// MAGIC 2. retrain the model with your ratings, them suggest some movies for yourself!
// MAGIC 3. Well, the movies are a little old, hope you are old enough to have watched some of them :-)

// COMMAND ----------

