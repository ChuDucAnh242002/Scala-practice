// Databricks notebook source
// MAGIC %md
// MAGIC # COMP.CS.320 Data-Intensive Programming, Exercise 1
// MAGIC
// MAGIC This exercise is mostly introduction to the Azure Databricks notebook system.
// MAGIC
// MAGIC There are some basic programming tasks that can be done in either Scala or Python. The final two tasks are very basic Spark related tasks.
// MAGIC
// MAGIC This is the **Scala** version, switch to the Python version if you want to do the tasks in Python.
// MAGIC
// MAGIC Each task has its own cell(s) for the code. Add your solutions to the cells. You are free to add more cells if you feel it is necessary. There are cells with test code or example output following most of the tasks that involve producing code.
// MAGIC
// MAGIC Don't forget to submit your solutions to Moodle.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 1 - Read tutorial
// MAGIC
// MAGIC Read the "[Basics of using Databricks notebooks](https://adb-7895492183558578.18.azuredatabricks.net/?o=7895492183558578#notebook/2974598884121429)" tutorial notebook.
// MAGIC Clone the tutorial notebook to your own workspace and run at least the first couple code examples.
// MAGIC
// MAGIC To get a point from this task, add "done" (or something similar) to the following cell (after you have read the tutorial).

// COMMAND ----------

// MAGIC %md
// MAGIC Task 1 is done

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 2 - Basic function
// MAGIC
// MAGIC Part 1:
// MAGIC
// MAGIC - Write a simple function `mySum` that takes two integer as parameters and returns their sum.
// MAGIC
// MAGIC Part 2:
// MAGIC
// MAGIC - Write a function `myTripleSum` that takes three integers as parameters and returns their sum.

// COMMAND ----------

def mySum (x:Int, y:Int):Int = {
  x + y
}
def myTripleSum (x:Int, y:Int, z:Int):Int = {
  x + y + z
}

// COMMAND ----------

// you can test your functions by running both the previous and this cell

val sum41 = mySum(20, 21)
sum41 == 41 match {
    case true => println(s"mySum: correct result: 20+21 = ${sum41}")
    case false => println(s"mySum: wrong result: ${sum41} != 41")
}
val sum65 = myTripleSum(20, 21, 24)
sum65 == 65 match {
    case true => println(s"myTripleSum: correct result: 20+21+24 = ${sum65}")
    case false => println(s"myTripleSum: wrong result: ${sum65} != 65")
}

println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 3 - Fibonacci numbers
// MAGIC
// MAGIC The Fibonacci numbers, `F_n`, are defined such that each number is the sum of the two preceding numbers. The first two Fibonacci numbers are:
// MAGIC
// MAGIC $$F_0 = 0 \qquad F_1 = 1$$
// MAGIC
// MAGIC In the following cell, write a **recursive** function, `fibonacci`, that takes in the index and returns the Fibonacci number. (no need for any optimized solution here)
// MAGIC

// COMMAND ----------

def fibonacci(index:Int):Int = {
  if (index <= 1) index
  else fibonacci(index - 1) + fibonacci(index -2)
}

// COMMAND ----------

val fibo6 = fibonacci(6)
fibo6 == 8 match {
    case true => println("correct result: fibonacci(6) == 8")
    case false => println(s"wrong result: ${fibo6} != 8")
}

val fibo11 = fibonacci(11)
fibo11 == 89 match {
    case true => println("correct result: fibonacci(11) == 89")
    case false => println(s"wrong result: ${fibo11} != 89")
}

println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 4 - Higher order functions 1
// MAGIC
// MAGIC - `map` function can be used to transform the elements of a list.
// MAGIC - `reduce` function can be used to combine the elements of a list.
// MAGIC
// MAGIC Part 1:
// MAGIC
// MAGIC - Using the `myList`as a starting point, use function `map` to calculate the cube of each element, and then use the reduce function to calculate the sum of the cubes.
// MAGIC
// MAGIC Part 2:
// MAGIC
// MAGIC - Using functions `map` and `reduce`, find the largest value for f(x)=1+9*x-x^2 when the input values x are the values from `myList`.

// COMMAND ----------

val myList: List[Int] = List(2, 3, 5, 7, 11, 13, 17, 19)
val cubeMyList: List[Int] = myList.map(v => v * v * v)
val cubeSum: Int = cubeMyList.reduceLeft((v1, v2) => v1 + v2)

def func(x:Int):Int = {
  1 + (9 * x) - (x * x)
}
val funcMyList: List[Int] = myList.map(v => func(v))
val largestValue: Int = funcMyList.reduce((v1, v2) => if (v1 > v2) v1 else v2)

println(s"Sum of cubes:                    ${cubeSum}")
println(s"Largest value of f(x)=1+9*x-x^2:    ${largestValue}")
println("==============================")

val task5 = "sheena is a punk rocker she is a punk punk"
    .split(" ")
    .map((_, 1))
    .groupBy(_._1)
    .mapValues(v => v.map(_._2).reduce(_+_))
    

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC Sum of cubes:                    15803
// MAGIC Largest value of f(x)=1+9*x-x^2:    21
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 5 - Higher order functions 2
// MAGIC
// MAGIC Explain the following code snippet. You can try the snippet piece by piece in a notebook cell or search help from Scaladoc ([https://www.scala-lang.org/api/2.12.x/](https://www.scala-lang.org/api/2.12.x/)).
// MAGIC
// MAGIC ```scala
// MAGIC "sheena is a punk rocker she is a punk punk"
// MAGIC     .split(" ")
// MAGIC     .map(s => (s, 1))
// MAGIC     .groupBy(p => p._1)
// MAGIC     .mapValues(v => v.length)
// MAGIC ```
// MAGIC
// MAGIC What about?
// MAGIC
// MAGIC ```scala
// MAGIC "sheena is a punk rocker she is a punk punk"
// MAGIC     .split(" ")
// MAGIC     .map((_, 1))
// MAGIC     .groupBy(_._1)
// MAGIC     .mapValues(v => v.map(_._2).reduce(_+_))
// MAGIC ```
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC The first one have a string which first split into multiple words: sheena, is, a, punk, ...
// MAGIC
// MAGIC The second function map create a tuple for each word which will have 1 as the second value: (sheena, 1), (is, 1), (a, 1), (punk, 1)
// MAGIC
// MAGIC The third function groupBy will group all the tuples which have the same first value and second value will be an array of tuple, the output value will be a Map[String, Array(String, Int)]: Map(sheena -> Array((sheena,1)), is -> Array((is,1), (is,1)), ...)
// MAGIC
// MAGIC The fourth function mapValues will map by the values's length which mean the array length: Map(sheena -> 1, is -> 2, ...)
// MAGIC
// MAGIC Overall, the purpose of the first one is to count the identical word in the string and return the result as a map.
// MAGIC
// MAGIC The second one also do the same job but instead of writing the value (for example: s, p, v), it uses _ indicates that it doesn't matter the input value. For example: the map doesn't need to know what the value v, it just create a tuple with the second value of 1.
// MAGIC
// MAGIC The mapValues function will get the second value of the tuple and calculate the sum instead of getting the length of the array only. This is a better way to calculate for the situation where the second value of the tuple isn't 1.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 6 - Approximation for fifth root
// MAGIC
// MAGIC Write a function, `fifthRoot`, that returns an approximate value for the fifth root of the input. Use the Newton's method, [https://en.wikipedia.org/wiki/Newton's_method](https://en.wikipedia.org/wiki/Newton%27s_method), with the initial guess of 1. For the fifth root this Newton's method translates to:
// MAGIC
// MAGIC $$y_0 = 1$$
// MAGIC $$y_{n+1} = \frac{1}{5}\bigg(4y_n + \frac{x}{y_n^4}\bigg) $$
// MAGIC
// MAGIC where `x` is the input value and `y_n` is the guess for the cube root after `n` iterations.
// MAGIC
// MAGIC Example steps when `x=32`:
// MAGIC
// MAGIC $$y_0 = 1$$
// MAGIC $$y_1 = \frac{1}{5}\big(4*1 + \frac{32}{1^4}\big) = 7.2$$
// MAGIC
// MAGIC $$y_2 = \frac{1}{5}\big(4*7.2 + \frac{32}{7.2^4}\big) = 5.76238$$
// MAGIC
// MAGIC $$y_3 = \frac{1}{5}\big(4*5.76238 + \frac{32}{5.76238^4}\big) = 4.61571$$
// MAGIC
// MAGIC $$y_4 = \frac{1}{5}\big(4*4.61571 + \frac{32}{4.61571^4}\big) = 3.70667$$
// MAGIC
// MAGIC $$...$$
// MAGIC
// MAGIC You will have to decide yourself on what is the condition for stopping the iterations. (you can add parameters to the function if you think it is necessary)
// MAGIC
// MAGIC Note, if your code is running for hundreds or thousands of iterations, you are either doing something wrong or trying to calculate too precise values.

// COMMAND ----------

def fifthRoot(x: Double, guess: Double = 1.0): Double = {
  val fr = Math.pow(Math.abs(x), 1.0/5.0)
  val error = Math.abs(guess - fr)
  val new_guess = (4*guess + Math.abs(x)/(Math.pow(guess,4))) / 5
  if(error < 1e-5) 
    if(x < 0) (-guess)
    else guess
  else fifthRoot(x, new_guess)
} 
  


println(s"Fifth root of 32:       ${fifthRoot(32)}")
println(s"Fifth root of 3125:     ${fifthRoot(3125)}")
println(s"Fifth root of 10^10:    ${fifthRoot(1e10)}")
println(s"Fifth root of 10^(-10): ${fifthRoot(1e-10)}")
println(s"Fifth root of -243:     ${fifthRoot(-243)}")
println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output
// MAGIC
// MAGIC (the exact values are not important, but the results should be close enough)
// MAGIC
// MAGIC ```text
// MAGIC Fifth root of 32:       2.0000000000000244
// MAGIC Fifth root of 3125:     5.000000000000007
// MAGIC Fifth root of 10^10:    100.00000005161067
// MAGIC Fifth root of 10^(-10): 0.010000000000000012
// MAGIC Fifth root of -243:     -3.0000000040240726
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 7 - First Spark task
// MAGIC
// MAGIC Create and display a DataFrame with your own data similarly as was done in the tutorial notebook.
// MAGIC
// MAGIC Then fetch the number of rows from the DataFrame.

// COMMAND ----------

import org.apache.spark.sql.DataFrame

val myData = Seq(
  ("Arsenal", 1886, 13),
  ("Chelsea", 1905, 6),
  ("Liverpool", 1892, 19),
  ("Manchester City", 1880, 9),
  ("Manchester United", 1878, 20),
  ("Tottenham Hotspur F.C.", 1882, 2)
)
val myDF: DataFrame = spark.createDataFrame(myData).toDF("Name", "Founded", "Titles")

myDF.show()

// COMMAND ----------

val numberOfRows: Long = myDF.count()

println(s"Number of rows in the DataFrame: ${numberOfRows}")
println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output
// MAGIC (the actual data can be totally different):
// MAGIC
// MAGIC ```text
// MAGIC +----------------------+-------+------+
// MAGIC |                  Name|Founded|Titles|
// MAGIC +----------------------+-------+------+
// MAGIC |               Arsenal|   1886|    13|
// MAGIC |               Chelsea|   1905|     6|
// MAGIC |             Liverpool|   1892|    19|
// MAGIC |       Manchester City|   1880|     9|
// MAGIC |     Manchester United|   1878|    20|
// MAGIC |Tottenham Hotspur F.C.|   1882|     2|
// MAGIC +----------------------+-------+------+
// MAGIC Number of rows in the DataFrame: 6
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Task 8 - Second Spark task
// MAGIC
// MAGIC The CSV file `numbers.csv` contains some data on how to spell numbers in different languages. The file is located in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) in folder `exercises/ex1`.
// MAGIC
// MAGIC Load the data from the file into a DataFrame and display it.
// MAGIC
// MAGIC Also, calculate the number of rows in the DataFrame.

// COMMAND ----------

val file_csv = "abfss://shared@tunics320f2024gen2.dfs.core.windows.net/exercises/ex1/numbers.csv"
val df_csv = spark.read
  .option("header", "true")
  .option("sep", ",") 
  .option("inferSchema", "true")
  .csv(file_csv)

display(df_csv)

// COMMAND ----------

val numberOfNumbers: Long = df_csv.count()

println(s"Number of rows in the number DataFrame: ${numberOfNumbers}")
println("==============================")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Example output:
// MAGIC
// MAGIC ```text
// MAGIC +------+-------+---------+-------+------+
// MAGIC |number|English|  Finnish|Swedish|German|
// MAGIC +------+-------+---------+-------+------+
// MAGIC |     1|    one|     yksi|    ett|  eins|
// MAGIC |     2|    two|    kaksi|    twå|  zwei|
// MAGIC |     3|  three|    kolme|    tre|  drei|
// MAGIC |     4|   four|    neljä|   fyra|  vier|
// MAGIC |     5|   five|    viisi|    fem|  fünf|
// MAGIC |     6|    six|    kuusi|    sex| sechs|
// MAGIC |     7|  seven|seitsemän|    sju|sieben|
// MAGIC |     8|  eight|kahdeksan|   åtta|  acht|
// MAGIC |     9|   nine| yhdeksän|    nio|  neun|
// MAGIC |    10|    ten| kymmenen|    tio|  zehn|
// MAGIC +------+-------+---------+-------+------+
// MAGIC Number of rows in the number DataFrame: 10
// MAGIC ```
