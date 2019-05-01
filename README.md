# Web Recomendation REST API

## Link
1. Getting model.recommendForAllUsers(count) : `localhost:5438/user-to-kindle/<int:count>`
2. Getting model.recommendForAllItems(count) : `localhost:5438/kindle-to-user/<int:count>`
3. Getting prediction of certain user & item : `localhost:5438/<int:user_id>/ratings/<int:movie_id>`

## Modification Required
1. Increase Spark Memory Limit to 8G (server.py)
Increate its memory to prevet error (OutOfMemoryError: GC overhead limit exceeded)
```
conf = SparkConf().setAppName("movie_recommendation-server").set("spark.executor.memory","8G").set("spark.driver.memory", "8G")
```
2. Add `SPARKSESSION` and  `SQLContext` to `SparkContext` (server.py)
Since i use dataframe-based (not RDD-based) so i need `SPARKSESSION` and `SQLContext` to do operation in `DATAFRAME`
```
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession

SparkSession(sc)
SQLContext(sc)
```

## Screenshoot of third link
![Prediction](https://github.com/abaar/spark-recomendation/blob/master/getting%20prediction%20for%20user%20%26%20id.PNG)

## Screenshoot of first & second link
P.S This is not the output but, it will generate the `recommendForAllUsers`. Since it need to much RESOURCES and TIME as the picture below, by CPU (I7 6700HQ up to 
3.50 GHz) and 12G of RAM, it took 8~10 MINS to compute (8+8/100). And based of the previous task [ALS Notebook](https://github.com/abaar/spark-recomendation/blob/master/Spark%20Recomendation%20(ALS)%20step%20by%20step%20.ipynb) , it took me about 1-2 Hours to generate recommendForAllUsers.
Then i decide to cancel it and do the THIRD LINK. But it WILL generate the output properly, it just not enough and resource to compute that.

![runtime](https://github.com/abaar/spark-recomendation/blob/master/generating%20recomendation.PNG)

![runtime2](https://github.com/abaar/spark-recomendation/blob/master/generating%20recomendation%202.PNG)
