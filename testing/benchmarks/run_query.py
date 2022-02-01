# Script for running a single query passed via command line.
# This can be used in future benchmarks to gauge the performance when using Python instead of "pure" SQL queries.

import sys

from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext()
sqlContext = SQLContext(sc)

query = str(sys.argv[1])

sqlContext.sql(query).show(999999)
