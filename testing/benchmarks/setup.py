from pyspark.sql import SQLContext
from pyspark import SparkContext

from pyspark.sql.utils import AnalysisException

input_home = "/input_home/"       # terminate directory path by '/'

airbnb_source = "airbnb.csv"
fueleconomy_source = "fueleconomy.csv"
coil2000_source = "coil2000.csv"
nba_source = "nba.csv"

store_sales_source ="store_sales.csv"

airbnb_source_incomplete = "airbnb_incomplete.csv"
fueleconomy_source_incomplete = "fueleconomy_incomplete.csv"
coil2000_source_incomplete = "coil2000_incomplete.csv"
nba_source_incomplete = "nba_incomplete.csv"

store_sales_incomplete_source ="store_sales.csv"

sc = SparkContext()
sqlContext = SQLContext(sc)

sqlContext.sql("CREATE DATABASE IF NOT EXISTS benchmarks")

try:
    df_airbnb = sqlContext.read.format('csv').options(header='true', inferschema='true').load(input_home + airbnb_source)
    df_airbnb.registerTempTable("airbnb")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.airbnb AS SELECT * FROM airbnb");
except AnalysisException as e:
    # skip file
    print("Inside AirBnB input file does not exist.")
    print(e)

try:
    df_fueleconomy = sqlContext.read.format('csv').options(header='true', inferschema='true').load(input_home + fueleconomy_source)
    df_fueleconomy.registerTempTable("fueleconomy")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.fueleconomy AS SELECT * FROM fueleconomy");
except AnalysisException as e:
    # skip file
    print("Fuel economy input file does not exist.")
    print(e)

try:
    df_coil2000 = sqlContext.read.format('csv').options(header='true', inferschema='true').load(input_home + coil2000_source)
    df_coil2000.registerTempTable("coil2000")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.coil2000 AS SELECT * FROM coil2000");
except AnalysisException as e:
    # skip file
    print("Coil 2000 input file does not exist.")
    print(e)

try:
    df_nba = sqlContext.read.format('csv').options(header='true', inferschema='true').load(input_home + nba_source)
    df_nba.registerTempTable("nba")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.nba AS SELECT * FROM nba");
except AnalysisException as e:
    # skip file
    print("NBA input file does not exist.")
    print(e)

try:
    df_airbnb_incomplete = sqlContext.read.format('csv').options(header='true', inferschema='true').load(input_home + airbnb_source_incomplete)
    df_airbnb_incomplete.registerTempTable("airbnb_incomplete")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.airbnb_incomplete AS SELECT * FROM airbnb_incomplete");
except AnalysisException as e:
    # skip file
    print("Inside AirBnB input file (incomplete) does not exist.")
    print(e)

try:
    df_fueleconomy_incomplete = sqlContext.read.format('csv').options(header='true', inferschema='true').load(input_home + fueleconomy_source_incomplete)
    df_fueleconomy_incomplete.registerTempTable("fueleconomy_incomplete")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.fueleconomy_incomplete AS SELECT * FROM fueleconomy_incomplete");
except AnalysisException as e:
    # skip file
    print("Fuel economy input file (incomplete) does not exist.")
    print(e)

try:
    df_coil2000_incomplete = sqlContext.read.format('csv').options(header='true', inferschema='true').load(input_home + coil2000_source_incomplete)
    df_coil2000_incomplete.registerTempTable("coil2000_incomplete")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.coil2000_incomplete AS SELECT * FROM coil2000_incomplete");
except AnalysisException as e:
    # skip file
    print("Coil 2000 input file (incomplete) does not exist.")
    print(e)

try:
    df_nba_incomplete = sqlContext.read.format('csv').options(header='true', inferschema='true').load(input_home + nba_source_incomplete)
    df_nba_incomplete.registerTempTable("nba_incomplete")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.nba_incomplete AS SELECT * FROM nba_incomplete");
except AnalysisException as e:
    # skip file
    print("NBA input file (incomplete) does not exist.")
    print(e)



try:
    df_store_sales_incomplete = sqlContext.read.format('csv').options(header='true', inferschema='true').load(input_home + store_sales_incomplete_source)
    df_store_sales_incomplete.registerTempTable("store_sales_incomplete")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.store_sales_incomplete_10 AS SELECT * FROM store_sales_incomplete LIMIT 1000000");
    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.store_sales_incomplete_20 AS SELECT * FROM store_sales_incomplete LIMIT 2000000");
    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.store_sales_incomplete_50 AS SELECT * FROM store_sales_incomplete LIMIT 5000000");

    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.store_sales_10 AS SELECT * FROM store_sales_incomplete \
                    WHERE ss_quantity IS NOT NULL AND \
                          ss_wholesale_cost IS NOT NULL AND \
                          ss_list_price IS NOT NULL AND \
                          ss_sales_price IS NOT NULL AND \
                          ss_ext_discount_amt IS NOT NULL AND \
                          ss_ext_sales_price IS NOT NULL \
                    LIMIT 1000000");
    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.store_sales_20 AS SELECT * FROM store_sales_incomplete \
                    WHERE ss_quantity IS NOT NULL AND \
                          ss_wholesale_cost IS NOT NULL AND \
                          ss_list_price IS NOT NULL AND \
                          ss_sales_price IS NOT NULL AND \
                          ss_ext_discount_amt IS NOT NULL AND \
                          ss_ext_sales_price IS NOT NULL \
                    LIMIT 2000000");
    sqlContext.sql("CREATE TABLE IF NOT EXISTS benchmarks.store_sales_50 AS SELECT * FROM store_sales_incomplete \
                    WHERE ss_quantity IS NOT NULL AND \
                          ss_wholesale_cost IS NOT NULL AND \
                          ss_list_price IS NOT NULL AND \
                          ss_sales_price IS NOT NULL AND \
                          ss_ext_discount_amt IS NOT NULL AND \
                          ss_ext_sales_price IS NOT NULL \
                    LIMIT 5000000");
except AnalysisException as e:
    # skip file
    print("Synthetic (store sales) input file (incomplete) does not exist.")
    print(e)