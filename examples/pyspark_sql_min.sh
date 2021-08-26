# simple examples
val df = spark.read.option("header", true).csv("/home/lukas/spark/data/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM hotels").show()
sqlcontext.sql("SELECT * FROM hotels WHERE price > 500 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").explain
sqlcontext.sql("SELECT * FROM hotels WHERE price > 500 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").explain("extended")

# example for logical query execution plan via queryExecution
# sqlcontext.sql("SELECT * FROM hotels WHERE price > 500 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").queryExecution.logical

# multiple dimensions
val df = spark.read.option("header", true).csv("/home/lukas/spark/data/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlContext.sql("SELECT * FROM hotels").show()
sqlContext.sql("SELECT * FROM hotels SKYLINE OF price MIN, distance MIN").show
sqlContext.sql("SELECT * FROM hotels SKYLINE OF price MIN, distance MIN").explain("extended")

# skyline and filter + order
val df = spark.read.option("header", true).csv("/home/lukas/spark/data/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").show
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").explain("extended")

# codegen skyline and filter + order
val df = spark.read.option("header", true).csv("/home/lukas/spark/data/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MAX ORDER BY distance DESC, price ASC").queryExecution.debug.codegen

# basic dataframe/dataset API (single dimension)
val df = spark.read.option("header", true).csv("/home/lukas/spark/data/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
df.filter(col("price") > 250).skyline(("price", "min")).show
df.filter(col("price") > 250).skyline(("price", "min")).explain("extended")
df.filter(col("price") > 250).skyline(col("price").smin).show
df.filter(col("price") > 250).skyline(col("price").smin).explain("extended")
df.filter(col("price") > 250).skyline(("price", "min", "distinct")).show
df.filter(col("price") > 250).skyline(("price", "min", "distinct")).explain("extended")
df.filter(col("price") > 250).skyline(col("price").smin.sdistinct).show
df.filter(col("price") > 250).skyline(col("price").smin.sdistinct).explain("extended")

# additional dataframe/dataset API (multiple dimensions)
val df = spark.read.option("header", true).csv("/home/lukas/spark/data/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
df.filter(col("price") > 250).skyline(("price", "min"), ("distance", "min")).show
df.filter(col("price") > 250).skyline(("price", "min"), ("distance", "min")).explain("extended")
df.filter(col("price") > 250).skyline(col("price").smin).show
df.filter(col("price") > 250).skyline(col("price").smin).explain("extended")
df.filter(col("price") > 250).skyline(("price", "min", "distinct"), ("distance", "min", "nondistinct")).show
df.filter(col("price") > 250).skyline(("price", "min", "distinct"), ("distance", "min", "nondistinct")).explain("extended")
df.filter(col("price") > 250).skyline(col("price").smin.sdistinct, col("distance").smin).show
df.filter(col("price") > 250).skyline(col("price").smin.sdistinct, col("distance").smin).explain("extended")

# advanced skyline query including a join and table aliases
val df = spark.read.option("header", true).csv("/home/lukas/spark/data/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val ratings = spark.read.option("header", true).csv("/home/lukas/spark/data/hotels_bahamas_ratings_min.csv")
ratings.registerTempTable("ratings")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM hotels h JOIN ratings r ON (h.id = r.hotel) SKYLINE OF h.price MIN, h.distance MIN").show
sqlcontext.sql("SELECT * FROM hotels h JOIN ratings r ON (h.id = r.hotel) SKYLINE OF h.price MIN, h.distance MIN").explain("extended")
