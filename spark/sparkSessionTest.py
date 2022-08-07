# import django

#print(django.get_version())

from pyspark import find_spark_home
import pyspark
from pyspark.sql.types import IntegerType()
from pyspark.sql.functions import col, current_timestamp

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.range(5)
df.show()
