from pyspark.sql import SparkSession

# from src.csv_read import spark

spark = SparkSession.builder.appName("setoperators").getOrCreate()

data1 = [(1,"Surendar",30),
        (2,"Ramanji",25),
        (3,"Srinu",23),
        (4,"Charan",24),
        (5,"Siddu",30)]

columns = ["id","Name","age"]
df1 = spark.createDataFrame(data1,columns)
# df1.show()
data2 = [(6,"Ritwik",30),
         (7,"Naveen",22),
         (4,"Charan",24),
         (3,"Srinu",23),
         (1,"Surendar",30),
         (2,"Ramanji",25),
         (9,"Gopi",28),
         (10,"Sai",30)]
df2 = spark.createDataFrame(data2,columns)
# df2.show()
# union
df_union = df1.union(df2)
# df_union.show()
# +---+--------+---+
# | id|    Name|age|
# +---+--------+---+
# |  1|Surendar| 30|
# |  2| Ramanji| 25|
# |  3|   Srinu| 23|
# |  4|  Charan| 24|
# |  5|   Siddu| 30|
# |  6|  Ritwik| 30|
# |  7|  Naveen| 22|
# |  4|  Charan| 24|
# |  3|   Srinu| 23|
# |  1|Surendar| 30|
# |  2| Ramanji| 25|
# |  9|    Gopi| 28|
# | 10|     Sai| 30|
# +---+--------+---+


# unionall
df_unionall = df1.unionAll(df2)
# df_unionall.show()
df_distinct = df_unionall.distinct()
# df_distinct.show()

# +---+--------+---+
# | id|    Name|age|
# +---+--------+---+
# |  1|Surendar| 30|
# |  2| Ramanji| 25|
# |  3|   Srinu| 23|
# |  4|  Charan| 24|
# |  5|   Siddu| 30|
# |  6|  Ritwik| 30|
# |  7|  Naveen| 22|
# |  9|    Gopi| 28|
# | 10|     Sai| 30|
# +---+--------+---+


# intersect

df_interset = df1.intersect(df2)
# df_interset.show()
# +---+--------+---+
# | id|    Name|age|
# +---+--------+---+
# |  3|   Srinu| 23|
# |  1|Surendar| 30|
# |  4|  Charan| 24|
# |  2| Ramanji| 25|
# +---+--------+---+

# except

df_except = df1.exceptAll(df2)
# df_except.show()
# +---+-----+---+
# | id| Name|age|
# +---+-----+---+
# |  5|Siddu| 30|
# +---+-----+---+

# set operators allways gives the records from the first dataframe only
