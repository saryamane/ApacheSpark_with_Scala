// Databricks notebook source exported at Sat, 12 Sep 2015 19:37:07 UTC
// MAGIC %md
// MAGIC # Assignment 1: Analyze Power Usage Data 
// MAGIC ## (Due on the Week 3 Class)

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Instructions 
// MAGIC **This is an Open-ended problem. I'm not looking for one correct answer, but an organized analysis report on the data. **
// MAGIC 
// MAGIC We will use a dataset from one smartmeter to analyze the energy consumption pattern of a house. Analyze the electricity usage pattern and see what conclusion you can draw about the resident?
// MAGIC 
// MAGIC Note:
// MAGIC 
// MAGIC 1. You need to pay attention to missing data;
// MAGIC 2. calculate some aggregate values of energy usage and observe different type of trends (e.g. pattern in a day, pattern in a week, pattern in a year, etc);
// MAGIC 3. Use Spark to do your calculations, then use dataframes to draw some plots. Describe each plot briefly and draw some conclusions;
// MAGIC 4. You only need to use the simple Spark transformations and actions covered in class, no need to use machine learning methods yet. 

// COMMAND ----------

// MAGIC %md
// MAGIC #### How to work on and hand in this assignment
// MAGIC Simply clone this Notebook to your own directory. Write your analysis report, and send me the link (see address bar) of your Notebook before the assignment is due. 
// MAGIC 
// MAGIC If you prefer to do it in Python (or R), you can create a Python/R Notebook and send me the link to it. 

// COMMAND ----------

// MAGIC %md
// MAGIC #### Description of the Dataset
// MAGIC Source: https://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption#
// MAGIC 
// MAGIC This archive contains 2075259 measurements gathered between December 2006 and November 2010 (47 months). 
// MAGIC 
// MAGIC Notes: 
// MAGIC 
// MAGIC 1.(global_active_power*1000/60 - sub_metering_1 - sub_metering_2 - sub_metering_3) represents the active energy consumed every minute (in watt hour) in the household by electrical equipment not measured in sub-meterings 1, 2 and 3. 
// MAGIC 
// MAGIC 2.The dataset contains some missing values in the measurements (nearly 1,25% of the rows). All calendar timestamps are present in the dataset but for some timestamps, the measurement values are missing: a missing value is represented by the absence of value between two consecutive semi-colon attribute separators. For instance, the dataset shows missing values on April 28, 2007.
// MAGIC 
// MAGIC 
// MAGIC Attribute Information:
// MAGIC 
// MAGIC 1.date: Date in format dd/mm/yyyy 
// MAGIC 
// MAGIC 2.time: time in format hh:mm:ss 
// MAGIC 
// MAGIC 3.global_active_power: household global minute-averaged active power (in kilowatt) 
// MAGIC 
// MAGIC 4.global_reactive_power: household global minute-averaged reactive power (in kilowatt) 
// MAGIC 
// MAGIC 5.voltage: minute-averaged voltage (in volt) 
// MAGIC 
// MAGIC 6.global_intensity: household global minute-averaged current intensity (in ampere) 
// MAGIC 
// MAGIC 7.sub_metering_1: energy sub-metering No. 1 (in watt-hour of active energy). It corresponds to the kitchen, containing mainly a dishwasher, an oven and a microwave (hot plates are not electric but gas powered). 
// MAGIC 
// MAGIC 8.sub_metering_2: energy sub-metering No. 2 (in watt-hour of active energy). It corresponds to the laundry room, containing a washing-machine, a tumble-drier, a refrigerator and a light. 
// MAGIC 
// MAGIC 9.sub_metering_3: energy sub-metering No. 3 (in watt-hour of active energy). It corresponds to an electric water-heater and an air-conditioner.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Load the data into an RDD

// COMMAND ----------

//Set S3 
sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAJIWXMZ5GH7WJ5UDQ")
sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

//read txt file, gzipped files will be auto-unzipped
val myRDD = sc.textFile("s3n://mlonspark/household_power_consumption.txt.gz")

println(myRDD.count())
myRDD.take(5).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Start Your Analysis Here

// COMMAND ----------

// MAGIC %md
// MAGIC Lets first aggregate the data on a year level to plot some simple bar charts to see how the overall usage have varied over a period of almost 4 years (i.e. from 2006 to 2010)

// COMMAND ----------

val df = myRDD.map(s => (s.split(";")(0).split("/").drop(2)(0),s.split(";")(2))).toDF("Year","Unit power")

display(df)


// COMMAND ----------

// MAGIC %md
// MAGIC Looks like we have some incomplete data for the year 2006 (we have only 1 month data). Also apparently there is some drop observed for the year 2010.
// MAGIC 
// MAGIC Based on the observation of the graph it looks like the electricity utilization is decreasing on an annual basis.
// MAGIC 
// MAGIC Even if we take the average of the year for 2010 (for a specific month) and add to the current total, we get the value of 525736.25
// MAGIC 
// MAGIC Let us plot for those adjusted values against the line chart to see trending over a period of time.

// COMMAND ----------

val df1 = sc.parallelize(Array((2007,582708.19),(2008,564893.09),(2009,562315.20),(2010,525736.26))).toDF("Year","Unit power adjusted")
display(df1)





// COMMAND ----------

// MAGIC %md
// MAGIC This shows that even though the data for 2010 is incomplete, after adjusting to its average it is still showing us a steep decline in power usage for 2010.
// MAGIC 
// MAGIC This can be attributed to some power saving methods implemented by the owner of the houshold like changing the old wiring, enrolling in smart plans and so on and so forth.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Plotting the data points on a day level
// MAGIC 
// MAGIC Now lets try to look for a random selected day, and try to find how the usage varies on an hourly basis.
// MAGIC 
// MAGIC Choosing 1/15/2007 as the randomly selected day for identifying the daily usage pattern.

// COMMAND ----------

myRDD.take(5).foreach(println)

// COMMAND ----------

val df2 = myRDD.map(s => (s.split(";")(0),s.split(";")(1),s.split(";")(2)))
.filter(line => line._1 == "15/1/2007")
.map(s => (s._2,s._3))
.toDF("Minutes on Jan 15th, 2007 (Monday)","Usage")

display(df2)


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Based on the chart shown above, it looks like the overall usage spikes at about 7am, from an almost nil usage.
// MAGIC 
// MAGIC This tells us that he starts most of the appliances at about 7am, telling us that his wake up time is about 7am everyday.
// MAGIC 
// MAGIC This may change on weekend though, lets pick up a weekend day and analyze his usage patterns on a weekend.

// COMMAND ----------

val df3 = myRDD.map(s => (s.split(";")(0),s.split(";")(1),s.split(";")(2)))
.filter(line => line._1 == "14/1/2007") // This is a Sunday
.map(s => (s._2,s._3))
.toDF("Minutes on Jan 14th, 2007 (Sunday)","Usage")

display(df3)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Apparently something weird usage pattern reflects on a Sunday. The peak is at about Saturday midnight, and there are constant spikes observed throughout the night. Maybe this customer had a party that night :)

// COMMAND ----------

val df4 = myRDD.map(s => (s.split(";")(0),s.split(";")(1),s.split(";")(2)))
.filter(line => line._1 == "13/1/2007") // This is a Saturday
.map(s => (s._2,s._3))
.toDF("Minutes on Jan 13th, 2007 (Saturday)","Usage")

display(df4)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Now lets us move to identifying his weekly usage patterns for the customer.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Let's pick a week's range for our analysis. We will hardcode those dates within our filter logic.

// COMMAND ----------

val df5 = myRDD.map(s => (s.split(";")(0),s.split(";")(2)))
.filter(method => (method._1=="13/1/2007" || method._1=="14/1/2007" || method._1=="15/1/2007" || method._1=="16/1/2007" || method._1=="17/1/2007") || method._1=="18/1/2007" || method._1=="19/1/2007")
.toDF("Day of the week -- Saturday -> Friday","Usage")

display(df5)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Now lets plot its Monthly usage patterns for the year 2007.

// COMMAND ----------

val df6 = myRDD.map(s => (s.split(";")(0),s.split(";")(2)))
.filter(method => (method._1.split("/").drop(2)(0)=="2007"))
.map(str => (str._1.split("/")(1).toInt,str._2))
.sortByKey(true)
.toDF("Month Number","Usage")

display(df6)


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Thus it looks like the peak season for energy utilization is in Winter i.e. January and December.
// MAGIC The demans is lowest in the month of July, but gradually increases month by month until December.
// MAGIC 
// MAGIC The trend shows a decrease in consumption starting May until July.

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC #### We will address the below questions on this dataset and the graphs observed.
// MAGIC ** What time of the day is the usage generally high?

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Based on observed charts above, it is clear that during weekdays, the owner of the house gets up at 7AM, whereas on the weekend it is around 8:30am.

// COMMAND ----------

// MAGIC %md
// MAGIC ** When does this owner usually do his laundry?

// COMMAND ----------

val df7 = myRDD.map(s => (s.split(";")(0),s.split(";")(7)))
.filter(method => (method._1=="13/1/2007" || method._1=="14/1/2007" || method._1=="15/1/2007" || method._1=="16/1/2007" || method._1=="17/1/2007") || method._1=="18/1/2007" || method._1=="19/1/2007")
.toDF("Day of the week -- Saturday -> Friday","Usage")

display(df7)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Lets look at one more data point to confirm our hypothesis.

// COMMAND ----------

val df8 = myRDD.map(s => (s.split(";")(0),s.split(";")(7)))
.filter(method => (method._1=="20/1/2007" || method._1=="21/1/2007" || method._1=="22/1/2007" || method._1=="23/1/2007" || method._1=="24/1/2007") || method._1=="25/1/2007" || method._1=="26/1/2007")
.toDF("Day of the week -- Saturday -> Friday","Usage")

display(df8)

// COMMAND ----------

val df9 = myRDD.map(s => (s.split(";")(0),s.split(";")(7)))
.filter(method => (method._1=="27/1/2007" || method._1=="28/1/2007" || method._1=="29/1/2007" || method._1=="30/1/2007" || method._1=="31/1/2007") || method._1=="1/2/2007" || method._1=="2/2/2007")
.toDF("Day of the week -- Thurs -> Wed","Usage")

display(df9)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Final comments: There is no fixed pattern for doing laundary. The owner usually does the laundary on either Saturday or Sunday, where we see highest consumption, and if not then Wednesday is his next preference of choice.

// COMMAND ----------

// MAGIC %md
// MAGIC ** does the owner prefer to eat out on certain days vs others? Metering on the kitchen usage bill.

// COMMAND ----------

val df10 = myRDD.map(s => (s.split(";")(0),s.split(";")(1),s.split(";")(6)))
.filter(line => line._1 == "15/1/2007")
.map(s => (s._2,s._3))
.toDF("Minutes on Jan 15th, 2007 (Monday)","Kitchen Usage")

display(df10)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Is there any correlation between Laundary and Kitchen for a certain day?

// COMMAND ----------

val df15 = myRDD.map(s => (s.split(";")(0),s.split(";")(1),s.split(";")(6),s.split(";")(7)))
.filter(line => line._1 == "20/1/2007")
.map(s => (s._2,s._3,s._4))
.toDF("Minutes on Jan 20th, 2007 (Saturday)","Kitchen Usage","Laundary Usage")

display(df15)

// COMMAND ----------

val df17 = myRDD.map(s => (s.split(";")(0),s.split(";")(1),s.split(";")(6),s.split(";")(7)))
.filter(line => line._1 == "21/1/2007")
.map(s => (s._2,s._3,s._4))
.toDF("Minutes on Jan 21th, 2007 (Sunday)","Kitchen Usage","Laundary Usage")

display(df17)

// COMMAND ----------

// MAGIC %md
// MAGIC Apparently there are patterns when the laundary and kitchen is being used simultaneously on Sunday's, but we did not see this happening on Saturday's result set.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Now lets look at the Kitchen and Laundary usage pattern over the period of the week

// COMMAND ----------

val df16 = myRDD.map(s => (s.split(";")(0),s.split(";")(6),s.split(";")(7)))
.filter(method => (method._1=="20/1/2007" || method._1=="21/1/2007" || method._1=="22/1/2007" || method._1=="23/1/2007" || method._1=="24/1/2007") || method._1=="25/1/2007" || method._1=="26/1/2007")
.toDF("Day of the week -- Saturday -> Friday","Kitchen Usage","Laundary Usage")

display(df16)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Final word: Apparently the owner prefered to do his laundary on the weekend, and he has no intersection of laundary with Kitchen, so he does not put his clothes for laundary and cook at the same time, as we see no overlap in the patter for that weekend day.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Does the owner prefer to eat out on certain days vs others? Metering on the kitchen usage bill, should give us that information.

// COMMAND ----------

val df10 = myRDD.map(s => (s.split(";")(0),s.split(";")(1),s.split(";")(6)))
.filter(line => line._1 == "15/1/2007")
.map(s => (s._2,s._3))
.toDF("Minutes on Jan 15th, 2007 (Monday)","Kitchen Usage")

display(df10)

// COMMAND ----------

val df12 = myRDD.map(s => (s.split(";")(0),s.split(";")(1),s.split(";")(6)))
.filter(line => line._1 == "14/1/2007")
.map(s => (s._2,s._3))
.toDF("Minutes on Jan 14th, 2007 (Sunday)","Kitchen Usage")

display(df12)

// COMMAND ----------

val df11 = myRDD.map(s => (s.split(";")(0),s.split(";")(6)))
.filter(method => (method._1=="20/1/2007" || method._1=="21/1/2007" || method._1=="22/1/2007" || method._1=="23/1/2007" || method._1=="24/1/2007") || method._1=="25/1/2007" || method._1=="26/1/2007")
.toDF("Day of the week -- Saturday -> Friday","Kitchen Usage")

display(df11)

// COMMAND ----------

val df12 = myRDD.map(s => (s.split(";")(0),s.split(";")(6)))
.filter(method => (method._1=="13/1/2007" || method._1=="14/1/2007" || method._1=="15/1/2007" || method._1=="16/1/2007" || method._1=="17/1/2007") || method._1=="18/1/2007" || method._1=="19/1/2007")
.toDF("Day of the week -- Saturday -> Friday","Usage")

display(df12)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Final Comments: From the chart it is apparent that the owner of the house prefers to eat out atleast 1 day a week. The day may vary.

// COMMAND ----------

// MAGIC %md
// MAGIC #### This concludes our analysis report.