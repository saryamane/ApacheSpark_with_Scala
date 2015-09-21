// Databricks notebook source exported at Mon, 21 Sep 2015 01:09:32 UTC
// MAGIC %md
// MAGIC # Trees and Forests
// MAGIC #### Dataset Introduction
// MAGIC This is a popular dataset for classification. Given a feature vector of 14 census results, the problem is to predict whether a persons income is greater than 50K.  

// COMMAND ----------

// MAGIC %md
// MAGIC #### Open Files (use traindata to train, testdata to test)

// COMMAND ----------

val trainFileRDD = sc.textFile("s3n://mlonspark/adult.traindata.numbers.csv")
val testFileRDD = sc.textFile("s3n://mlonspark/adult.testdata.numbers.csv")

trainFileRDD.take(10).foreach(println)
println
testFileRDD.take(10).foreach(println)


// COMMAND ----------

// MAGIC %md
// MAGIC #### Description of Fields
// MAGIC Note: For all categorial data, the number the number corresponds to a category. I.e. 1 = "Private", 2="Self-emp-not-inc" for the workclass (2nd) column.
// MAGIC 
// MAGIC * 0-age: continuous.
// MAGIC * 1-workclass: Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked.
// MAGIC * 2-fnlwgt: continuous.
// MAGIC * 3-education: Bachelors, Some-college, 11th, HS-grad, Prof-school, Assoc-acdm, Assoc-voc, 9th, 7th-8th, 12th, Masters, 1st-4th, 10th, Doctorate, 5th-6th, Preschool.
// MAGIC * 4-education-num: continuous.
// MAGIC * 5-marital-status: Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse.
// MAGIC * 6-occupation: Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving, Priv-house-serv, Protective-serv, Armed-Forces.
// MAGIC * 7-relationship: Wife, Own-child, Husband, Not-in-family, Other-relative, Unmarried.
// MAGIC * 8-race: White, Asian-Pac-Islander, Amer-Indian-Eskimo, Other, Black.
// MAGIC * 9-sex: Female, Male.
// MAGIC * 10-capital-gain: continuous.
// MAGIC * 11-capital-loss: continuous.
// MAGIC * 12-hours-per-week: continuous.
// MAGIC * 13-native-country: United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc), India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica, Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti, Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador, Trinadad&Tobago, Peru, Hong, Holand-Netherlands.
// MAGIC * 14-income: >50K, <=50K

// COMMAND ----------

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils



// COMMAND ----------

// MAGIC %md
// MAGIC #### Create LabeledPoint RDD from the data (Both training and test datasets)

// COMMAND ----------

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

val train_dataset = trainFileRDD.map { line =>
  val values = line.split(',').map(_.toDouble)
  val featureVector = Vectors.dense(values.init)
  val label = values.last - 1
  LabeledPoint(label, featureVector)
}

val test_dataset = testFileRDD.map { line =>
  val values = line.split(',').map(_.toDouble)
  val featureVector = Vectors.dense(values.init)
  val label = values.last - 1
  LabeledPoint(label, featureVector)
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Train a Decision Tree
// MAGIC * Use gini impurity, maxdepth=5, maxBins=100
// MAGIC * Refer to here: http://spark.apache.org/docs/latest/mllib-decision-tree.html#classification
// MAGIC * for categoricalFeaturesInfo, you need to make it similar to Map\[Int, Int\]((1,9),(3,17)), this means the 1st column is categorial and has 9 different categories, and the 3rd column is categorial and has 17 different categories. Note: we start counting from 2. 

// COMMAND ----------

// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]((1,9),(3,17),(5,8),(6,15),(7,7),(8,6),(9,3))
val impurity = "gini"
val maxDepth = 5
val maxBins = 40
val numTrees = 10
val featureSubsetStrategy = "auto"

val model = RandomForest.trainClassifier(train_dataset, numClasses, categoricalFeaturesInfo,numTrees,featureSubsetStrategy, impurity, maxDepth, maxBins)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Print the model to screen

// COMMAND ----------

// Save and load model
print(model)

println("Learned regression tree model:\n" + model.toDebugString)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Calculate Test Error
// MAGIC I.e. (wrong predictions/total predictions)

// COMMAND ----------

// Evaluate model on test instances and compute test error
val labelsAndPredictions = test_dataset.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}

val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
println("Test Mean Squared Error = " + testMSE)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Train Random Forest and Calculate Error
// MAGIC Use 30 trees, maxdepth=10, maxBins=100

// COMMAND ----------

// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]((1,9),(3,17),(5,8),(6,15),(7,7),(8,6),(9,3))
val impurity = "gini"
val maxDepth = 10
val maxBins = 100
val numTrees = 30
val featureSubsetStrategy = "auto"

val model = RandomForest.trainClassifier(train_dataset, numClasses, categoricalFeaturesInfo,numTrees,featureSubsetStrategy, impurity, maxDepth, maxBins)

// COMMAND ----------

// Let's compute the error rate of this 30 tree forest with a max dept of 10 and max bins of 100.

val labelsAndPredictions = test_dataset.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}

val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
println("Test Mean Squared Error = " + testMSE)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Vary "Number of Trees" and "Max Tree Depth" and observe Error Rate
// MAGIC Train with Number of Trees, 1,5,10,20,30, and Max Tree Depth 2,3,5,10 to train the random forest model, and draw a plot showing how the error changes

// COMMAND ----------

val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]((1,9),(3,17),(5,8),(6,15),(7,7),(8,6),(9,3))
val impurity = "gini"
// val maxDepth = 10
val maxBins = 100
// val numTrees = 30
val featureSubsetStrategy = "auto"


val iterateList = List((1,2),(5,3),(10,5),(20,10),(30,20))
for ((a,b) <- iterateList) {
  val model = RandomForest.trainClassifier(train_dataset, numClasses, categoricalFeaturesInfo,a,featureSubsetStrategy, impurity, b, maxBins)
  val labelsAndPredictions = test_dataset.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}

val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
println("Test Mean Squared Error = " + testMSE);
}

// COMMAND ----------

val arrayOfTuple = sc.parallelize(Array((1,0.17536520584329307),(2,0.17350597609561774 ),(3,0.15351925630810093 ),(4,0.14030544488711838),(5,0.14003984063745037)),2)

// COMMAND ----------

val df1 = arrayOfTuple.toDF("Iteration on Trees","MSE score")

// COMMAND ----------

display(df1)

// COMMAND ----------

