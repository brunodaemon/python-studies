import numpy as np
# Sets up the data for the schema,
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

names = ['Peter', 'Barbara', 'Shack', 'Lucius', 'Victor']
grades = [int(v) for v in np.random.randint(1, 11, 5)]

# Creates a DataFrame
data = [(names, grades) for names, grades in zip(names, grades)]
schema = 'names STRING, grades INT'
df_custom = spark.createDataFrame(data=data, schema=schema)
df_custom.show()
# Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set