import sys

from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext()
sqlContext = SQLContext(sc)

query = str(sys.argv[1])

sqlContext.sql(query).show(999999)
