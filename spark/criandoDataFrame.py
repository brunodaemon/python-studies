import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, round

spark = SparkSession.builder.getOrCreate()

names = ['Peter', 'Barbara', 'Shack', 'Lucius', 'Victor']
grades1 = [int(v) for v in np.random.randint(1, 11, 5)]
grades2 = [int(v) for v in np.random.randint(1, 11, 5)]
grades3 = [int(v) for v in np.random.randint(1, 11, 5)]
grades4 = [int(v) for v in np.random.randint(1, 11, 5)]

# Creates a DataFrame
data = [(names, grades1, grades2, grades3, grades4)
        for names, grades1, grades2, grades3, grades4
        in zip(names, grades1, grades2, grades3, grades4)]
schema = 'names STRING, grades1 INT, grades2 INT, grades3 INT, grades4 INT'
df_custom = spark.createDataFrame(data=data, schema=schema)
df_custom.withColumn('Total', col('grades1')+col('grades2')+col('grades3')+col('grades4')).show()
df_custom.withColumn('Avg', col('grades1')+col('grades2')+col('grades3')+col('grades4')/4).show()
df_custom.head()
df_custom.show()

