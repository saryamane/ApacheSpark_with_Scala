// Databricks notebook source exported at Sat, 12 Sep 2015 19:34:30 UTC
// MAGIC %md
// MAGIC # Demo of Basic Spark Transformations and Actions

// COMMAND ----------

// MAGIC %md
// MAGIC ## Single-RDD Transformations

// COMMAND ----------

// MAGIC %md
// MAGIC #### Filter

// COMMAND ----------

val res=sc.parallelize(1 to 100)
.filter(_%10==1)
.collect()

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Sample

// COMMAND ----------

// true means with replacement
// 0.1 is select 10% of the actual data. Its sample is 10%, not actual.
// random seed is 1, it will use a different seed each time if not specified
val res=sc.parallelize(1 to 10)
.sample(true,0.8,2)
.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC #### flatMap vs Map

// COMMAND ----------

val ll=Array("i am a sheep","i am mcdonald");

val res=sc.parallelize(ll)
.map(_.split(" "))
.collect()

res.length


// COMMAND ----------

val ll=Array("i am a sheep","i am mcdonald");

val res=sc.parallelize(ll)
.flatMap(_.split(" "))
.collect()

res.length


// COMMAND ----------

val a = List("cat","dog").zipWithIndex
a.take(5)

// COMMAND ----------

val a = List("Sami","Jerry","Tim","Sami")
val b = List(1,2,3,4)
val c = a.zip(b)

c.foreach(println)



// COMMAND ----------

// MAGIC %md
// MAGIC #### mapPartitionsWithIndex

// COMMAND ----------

def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
  iter.toList.map(x => "[partID:" + index + ", val: " + x + "]").iterator
}

sc.parallelize(List(1,2,3,4,5,6))
.mapPartitionsWithIndex(myfunc).collect

// COMMAND ----------

// MAGIC %md
// MAGIC #### Distinct

// COMMAND ----------

val ll=Array("i am a sheep","i am mcdonald");

val res=sc.parallelize(ll)
.flatMap(_.split(" "))
.distinct()
.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2-RDD Transformations

// COMMAND ----------

// MAGIC %md
// MAGIC #### Union

// COMMAND ----------

sc.parallelize(1 to 7)
.union(
sc.parallelize(5 to 8)
)
.collect()

//Union does not do de-dup.
// Its a union set operation.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Intersection

// COMMAND ----------

sc.parallelize(1 to 7)
.intersection(
sc.parallelize(5 to 8)
)
.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Subtract

// COMMAND ----------

sc.parallelize(1 to 7,2)
.subtract(
sc.parallelize(5 to 8,2)
)
.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Cartesian (cross product)

// COMMAND ----------

sc.parallelize(1 to 7)
.cartesian(
sc.parallelize(5 to 8)
)
.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Actions

// COMMAND ----------

// MAGIC %md
// MAGIC #### count

// COMMAND ----------

sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
.count

// COMMAND ----------

// MAGIC %md
// MAGIC #### countByValue

// COMMAND ----------

sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
.countByValue

// COMMAND ----------

// MAGIC %md
// MAGIC #### take/takeOrdered/takeSample

// COMMAND ----------

sc.parallelize(List("dog", "cat", "ape", "salmon", "gnu"), 2)
.take(2)

// COMMAND ----------

sc.parallelize(List("dog", "cat", "ape", "salmon", "gnu"), 2)
.takeOrdered(2)

// COMMAND ----------

sc.parallelize(List("dog", "cat", "ape", "salmon", "gnu"), 2)
.takeSample(false,4)

// COMMAND ----------

sc.parallelize(List("dog", "cat", "ape", "salmon", "gnu"), 2)
.top(2)

// COMMAND ----------

// MAGIC %md
// MAGIC #### top

// COMMAND ----------

sc.parallelize(Array(6, 9, 4, 7, 5, 8), 1)
.top(2)

// COMMAND ----------

// MAGIC %md
// MAGIC #### collect

// COMMAND ----------

sc.parallelize(Array(6, 9, 4, 7, 5, 8), 2)
.collect()
.foreach(println)

// COMMAND ----------

// MAGIC %md 
// MAGIC #### foreach

// COMMAND ----------

sc.parallelize(List("cat", "dog", "tiger", "lion", "gnu", "crocodile", "ant", "whale", "dolphin", "spider"), 3)
.map(x => (x,x.length))
.collect()

// COMMAND ----------

sc.parallelize(List("cat", "dog", "tiger", "lion", "gnu", "crocodile", "ant", "whale", "dolphin", "spider"), 3)
.map(x => (x,x.length))
.reduceByKey(_+_)
.collect()
.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC #### reduce

// COMMAND ----------

sc.parallelize(1 to 100, 3)
.reduce(_ + _)

// COMMAND ----------

// MAGIC %md
// MAGIC #### fold

// COMMAND ----------

sc.parallelize(1 to 10, 2)
.fold(1)(_ + _)

// COMMAND ----------

sc.parallelize(1 to 10, 2)
.fold(1)(_ + _)

// COMMAND ----------

sc.parallelize(1 to 10, 3)
.fold(1)(_ + _)

// COMMAND ----------

// MAGIC %md
// MAGIC #### aggregate

// COMMAND ----------

def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
  iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
}

val z = sc.parallelize(List(1,2,3,4,5,6), 2)

val partstr=z.mapPartitionsWithIndex(myfunc).collect

println(z.aggregate(0)(math.max(_, _), _ + _))

// This example returns 16 since the initial value is 5
// reduce of partition 0 will be max(5, 1, 2, 3) = 5
// reduce of partition 1 will be max(5, 4, 5, 6) = 6
// final reduce across partitions will be 5 + 5 + 6 = 16
// note the final reduce include the initial value
println(z.aggregate(5)(math.max(_, _), _ + _))


// COMMAND ----------

// MAGIC %md
// MAGIC ## Key-value pair Transformations

// COMMAND ----------

// MAGIC %md
// MAGIC #### reduceByKey

// COMMAND ----------

sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
.map(x => (x.length, x)).reduceByKey(_ + _).collect

// COMMAND ----------

// MAGIC %md
// MAGIC #### groupByKey

// COMMAND ----------

sc.parallelize(List("dog", "tiger", "lion","lion", "cat", "panther", "eagle"), 2)
.map(x => ( x,x.length)).groupByKey().collect

// COMMAND ----------

// MAGIC %md
// MAGIC #### mapValues

// COMMAND ----------

sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
.map(x => (x.length, x))
.mapValues("x" + _ + "x").collect

// COMMAND ----------

// MAGIC %md
// MAGIC #### flatMapValues

// COMMAND ----------

sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
.map(x => (x.length, x))
.flatMapValues("x" + _ + "x").collect

// COMMAND ----------

// MAGIC %md
// MAGIC #### keys/values

// COMMAND ----------

val a=sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2).map(x => (x.length, x))
a.collect.foreach(println)
a.keys.collect.foreach(println)
a.values.collect.foreach(println)
//Note: keys and values are RDDs

// COMMAND ----------

// MAGIC %md
// MAGIC #### sortByKey

// COMMAND ----------

val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
val b = sc.parallelize(1 to a.count.toInt, 2)
val c = a.zip(b)
c.sortByKey(true).collect

// COMMAND ----------

// MAGIC %md
// MAGIC #### join/leftOuterJoin/rightOuterJoin

// COMMAND ----------

val v1 = sc.parallelize(List("dog","salmon","salmon","rat","elephant"),3)
.keyBy(_.length)

v1.collect.foreach(println)
println
val v2 = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
.keyBy(_.length)
v2.collect.foreach(println)
println
v1.join(v2).collect.foreach(println)

println
println

v1.leftOuterJoin(v2).collect.foreach(println)

println
println

v1.rightOuterJoin(v2).collect.foreach(println)

// COMMAND ----------

val v1 = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
.keyBy(_.length)
v1.collect.foreach(println)
println
val v2 = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
.keyBy(_.length)
v2.collect.foreach(println)
println

v1.join(v2).collect.foreach(println)

// COMMAND ----------

v1.leftOuterJoin(v2).collect.foreach(println)

// COMMAND ----------

v1.rightOuterJoin(v2).collect.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC #### subtractByKey

// COMMAND ----------

val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
val b = a.keyBy(_.length)
b.collect.foreach(println)
println

val c = sc.parallelize(List("ant", "falcon", "squid", "helloworld"), 2)
val d = c.keyBy(_.length)
d.collect.foreach(println)
println

b.subtractByKey(d).collect.foreach(println)

// COMMAND ----------

val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
val b = a.keyBy(_.length)
b.collect.foreach(println)
println

val c = sc.parallelize(List("ant", "falcon", "squid", "helloworld"), 2)
val d = c.keyBy(_.length)
d.collect.foreach(println)
println

b.subtractByKey(d).collect.foreach(println)


// COMMAND ----------

// MAGIC %md
// MAGIC # From Tuple to Dataframes

// COMMAND ----------

val a = sc.parallelize(List("USA", "China", "Japan", "India", "France", "Big Britain"), 2)
val b = a.keyBy(_.length).map(i => (i._1*i._1, i._1,i._2))
b.collect.foreach(println)

val df = b.toDF("square_number","actual number","country name")
display(df)

// COMMAND ----------

val a = sc.parallelize(List("USA", "China", "Japan", "India", "France", "Big Britain"), 2)
val b = a.keyBy(_.length).map(i=>(i._1*i._1, i._1, i._2))
b.collect.foreach(println)
println


// COMMAND ----------

val df=b.toDF("length2","length","word")
display(df)

// COMMAND ----------

