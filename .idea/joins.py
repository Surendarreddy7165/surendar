from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("joins_practice").getOrCreate()

data1 = [(1,),(2,),(3,),(4,),(5,),(1,),(2,)]
schema1 = StructType([StructField("id",IntegerType(),True)])
df1 = spark.createDataFrame(data1,schema1)
# df1.show()
data2 = [(1,),(2,),(4,),(6,),(1,),(0,)]
schema2 = StructType([StructField("id",IntegerType(),True)])
df2 = spark.createDataFrame(data2,schema2)
# df2.show()
innerjoin_df = df1.join(df2,df1.id == df2.id ,"inner")
leftjoin_df = df1.join(df2,df1.id == df2.id,"left")
rigtjoin_df = df2.join(df1,df2.id == df1.id,"right")
crossjoin_df = df1.crossJoin(df2)
fullouterjoin_df = df1.join(df2,df1.id == df2.id,"full")
fullouterjoin_df.show()
innerjoin_df.show()
leftjoin_df.show()
rigtjoin_df.show()
crossjoin_df.show(42)
# fullouterjoin_df.show()
# +----+----+
# |  id|  id|
# +----+----+
# |NULL|   0|
# |   1|   1|
# |   1|   1|
# |   1|   1|
# |   1|   1|
# |   2|   2|
# |   2|   2|
# |   3|NULL|
# |   4|   4|
# |   5|NULL|
# |NULL|   6|
# +----+----+
# innerjoin_df.show()
# +---+---+
# | id| id|
# +---+---+
# |  1|  1|
# |  1|  1|
# |  1|  1|
# |  1|  1|
# |  2|  2|
# |  2|  2|
# |  4|  4|
# +---+---+
# leftjoin_df.show()
# +---+----+
# | id|  id|
# +---+----+
# |  1|   1|
# |  1|   1|
# |  2|   2|
# |  3|NULL|
# |  4|   4|
# |  5|NULL|
# |  1|   1|
# |  1|   1|
# |  2|   2|
# +---+----+

# rigtjoin_df.show()
# +----+---+
# |  id| id|
# +----+---+
# |   1|  1|
# |   1|  1|
# |   2|  2|
# |NULL|  3|
# |   4|  4|
# |NULL|  5|
# |   1|  1|
# |   1|  1|
# |   2|  2|
# +----+---+
# crossjoin_df
# +---+---+
# | id| id|
# +---+---+
# |  1|  1|
# |  1|  2|
# |  1|  4|
# |  1|  6|
# |  1|  1|
# |  1|  0|
# |  2|  1|
# |  2|  2|
# |  2|  4|
# |  2|  6|
# |  2|  1|
# |  2|  0|
# |  3|  1|
# |  3|  2|
# |  3|  4|
# |  3|  6|
# |  3|  1|
# |  3|  0|
# |  4|  1|
# |  4|  2|
# |  4|  4|
# |  4|  6|
# |  4|  1|
# |  4|  0|
# |  5|  1|
# |  5|  2|
# |  5|  4|
# |  5|  6|
# |  5|  1|
# |  5|  0|
# |  1|  1|
# |  1|  2|
# |  1|  4|
# |  1|  6|
# |  1|  1|
# |  1|  0|
# |  2|  1|
# |  2|  2|
# |  2|  4|
# |  2|  6|
# |  2|  1|
# |  2|  0|
# +---+---+



