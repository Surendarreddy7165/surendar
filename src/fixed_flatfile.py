from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, substring
spark = SparkSession.builder.appName("fixed width flat file").getOrCreate()
path = "C:\\Users\\rajas\\Desktop\\emp_fixed.txt"

df =spark.read.text(path)

df_fixed_width = df.withColumn("emp_id", substring("value", 1, 5)) \
                   .withColumn("emp_name", substring("value", 6, 10)) \
                   .withColumn("salary", substring("value", 16, 8)) \
                   .withColumn("department", substring("value", 24, 4)) \
                   .withColumn("year_of_joining", substring("value", 28, 5))

# Cast columns to appropriate types
df_fixed_width = df_fixed_width.withColumn("salary", df_fixed_width["salary"].cast("int")) \
                               .withColumn("year_of_joining", df_fixed_width["year_of_joining"].cast("int"))


df_fixed_width.show(truncate=False)
