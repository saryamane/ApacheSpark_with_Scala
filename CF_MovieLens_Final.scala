// Databricks notebook source exported at Sun, 13 Sep 2015 03:01:57 UTC
// MAGIC %md
// MAGIC # Collaborative Filtering on 100K MovieLens Dataset

// COMMAND ----------

// MAGIC %md
// MAGIC #### Load data from S3 and Create RDDs

// COMMAND ----------

sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAJIWXMZ5GH7WJ5UDQ")
sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")
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
// MAGIC #### Lets inspect the data elements from the u.data file

// COMMAND ----------

ratingRDD.first()

// COMMAND ----------

val rawRatings = ratingRDD.map(_.split("\t").take(3))
rawRatings.first()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Train the model using ALS
// MAGIC Use rank=20, #iterations=15, lambda=0.01
// MAGIC 
// MAGIC Example Here:
// MAGIC http://spark.apache.org/docs/latest/mllib-collaborative-filtering.html

// COMMAND ----------

ALS.train

// COMMAND ----------

Rating()

// COMMAND ----------

// MAGIC %md
// MAGIC Lets create a rating variable, with the userid, movieid and the actual rating elements.
// MAGIC We'll create our rating dataset using the map method and transforming the array of IDs and ratings into a Rating object.

// COMMAND ----------

val ratings = rawRatings.map { case Array(user,movie,rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }

// COMMAND ----------

// MAGIC %md
// MAGIC Now we have the Rating RDD which we can call by calling ratings.first()

// COMMAND ----------

ratings.first()

// COMMAND ----------

// MAGIC %md
// MAGIC Lets now train our model by first declaring proper input variables.

// COMMAND ----------

val rank = 200
val iterations = 15
val lambda = 0.01

// COMMAND ----------

// MAGIC %md
// MAGIC Training our model

// COMMAND ----------

val model = ALS.train(ratings, rank, iterations, lambda)

// COMMAND ----------

model.userFeatures

// COMMAND ----------

model.userFeatures.count

// COMMAND ----------

model.productFeatures.count

// COMMAND ----------

// MAGIC %md
// MAGIC Thus, we conclude that we have 943 user features (i.e. users) and 1682 movie features, which are the product.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Predict the rating of user 196 on item 557

// COMMAND ----------

model.predict(196,557)

// COMMAND ----------

val predictRating = model.predict(789,123)

// COMMAND ----------

// MAGIC %md
// MAGIC Thus we see that the model predicts a rating of 3.27 for a user id of 789 for the product id of 123.

// COMMAND ----------

// prediction for top K products for a certain user, the MatrixFactorization provides a handy function called recommendProducts

val userId = 789
val k = 10

val topKRecs = model.recommendProducts(userId, k)

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

val ratesAndPreds = ratings.map { case Rating(user, product, rate) => 
  ((user, product), rate)
}.join(predictions)


// COMMAND ----------

predictions.first()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Calculate Mean Square Error
// MAGIC Hint: Copy/paste from the example in the link above :-)

// COMMAND ----------

val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => 
  val err = (r1 - r2)
  err * err
}.mean()
println("Mean Squared Error = " + MSE)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC #### Now suggest top 5 items to user 196 (and print the movie names as well)
// MAGIC Hint: rate all items, join with the itemRDD, and return the highest rating 5 (note that don't return the ones the user already rated!)

// COMMAND ----------

val k = 5
val userId = 196

val top5Products = model.recommendProducts(userId,k)
top5Products.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC #### MSE vs. Iterations
// MAGIC 
// MAGIC Use Number of iterations=1,2,3,5,7,9,11, retrain the model, and plot the number of iterations vs. MSE (Shouldn't take more than 30 seconds to run)
// MAGIC I.e. Observe the MSE going down when the #of iterations go up
// MAGIC 
// MAGIC Also change the k and lambda numbers to observe the difference

// COMMAND ----------

// rawRatings = Contains the Array(Int, Int, Int)
// ratings = Is the Rating object with Array(user, movie, rating)
// Then we do assign rank, iterations and lambda values to train the model using ALS.train
// Then we do the predictions on the list for the assigned iteration score.
// Calculate the MSE on that rates and pred values.

// COMMAND ----------

val rank = 20
val lambda = 0.01

val usersProducts = ratings.map { case Rating(user, product, rate) =>
  (user, product)
}

val iterateList = List(1,2,3,5,7,9,11)
for (a <- iterateList) {
  val model = ALS.train(ratings, rank, a, lambda)
  val predictions = 
  model.predict(usersProducts).map { case Rating(user, product, rate) => 
    ((user, product), rate)
  }
val ratesAndPreds = ratings.map { case Rating(user, product, rate) => 
  ((user, product), rate)
}.join(predictions)
val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => 
  val err = (r1 - r2)
  err * err
}.mean()
println("For iteration " + a + " the mean squared error is = " + MSE);
}

// COMMAND ----------

val arrayOfTuple = sc.parallelize(Array((1,2.227288347394307),(2,0.46945280424834623),(3,0.3915279780275785),(5,0.3401943477356818),(7,0.32035168688090926),(9,0.31004162406586216),(11,0.30432759342223253)),2)

// COMMAND ----------

arrayOfTuple.toDF("Iteration","MSE score")

// COMMAND ----------

val df1 = arrayOfTuple.toDF("Iteration","MSE value")

display(df1)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Suggest some movies for yourself and see if it really works!
// MAGIC Hints:
// MAGIC 1. look into the itemRDD, find some movies that you watch before, and rate them!
// MAGIC 2. retrain the model with your ratings, them suggest some movies for yourself!
// MAGIC 3. Well, the movies are a little old, hope you are old enough to have watched some of them :-)

// COMMAND ----------

