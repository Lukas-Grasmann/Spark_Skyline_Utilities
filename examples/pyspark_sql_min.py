# File with examples to be used in pyspark (copy individually as needed)
# TODO: replace <path> with the (absolute) path to the .csv files

# simple examples
val df = spark.read.option("header", true).csv("<path>/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM hotels").show
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MIN").show
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").explain
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").explain("extended")

# simple examples; distinct
val df = spark.read.option("header", true).csv("<path>/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM hotels").show()
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF DISTINCT price MIN ORDER BY distance DESC, price ASC").explain
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF DISTINCT price MIN ORDER BY distance DESC, price ASC").explain("extended")

# example for logical query execution plan via queryExecution
# sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").queryExecution.logical

# multiple dimensions
val df = spark.read.option("header", true).csv("<path>/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM hotels").show()
sqlcontext.sql("SELECT * FROM hotels SKYLINE OF price MIN, distance MIN").show
sqlcontext.sql("SELECT * FROM hotels SKYLINE OF price MIN, distance MIN").explain("extended")
sqlcontext.sql("SELECT * FROM hotels SKYLINE OF price MIN, distance MIN").queryExecution.debug.codegen

# skyline and filter + order
val df = spark.read.option("header", true).csv("<path>/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").show
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").explain("extended")

# codegen skyline and filter + order
val df = spark.read.option("header", true).csv("<path>/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MAX ORDER BY distance DESC, price ASC").queryExecution.debug.codegen

# basic dataframe/dataset API (single dimension)
val df = spark.read.option("header", true).csv("<path>/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
df.filter(col("price") > 250).skyline(("price", "min")).show
df.filter(col("price") > 250).skyline(("price", "min")).explain("extended")
df.filter(col("price") > 250).skyline(col("price").smin).show
df.filter(col("price") > 250).skyline(col("price").smin).explain("extended")
df.filter(col("price") > 250).skylineDistinct(("price", "min")).show
df.filter(col("price") > 250).skylineDistinct(("price", "min")).explain("extended")
df.filter(col("price") > 250).skylineDistinct(col("price").smin).show
df.filter(col("price") > 250).skylineDistinct(col("price").smin).explain("extended")

# additional dataframe/dataset API (multiple dimensions)
val df = spark.read.option("header", true).csv("<path>/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
df.filter(col("price") > 250).skyline(("price", "min"), ("distance", "min")).show
df.filter(col("price") > 250).skyline(("price", "min"), ("distance", "min")).explain("extended")
df.filter(col("price") > 250).skyline(col("price").smin).show
df.filter(col("price") > 250).skyline(col("price").smin).explain("extended")
df.filter(col("price") > 250).skylineDistinct(("price", "min"), ("distance", "min")).show
df.filter(col("price") > 250).skylineDistinct(("price", "min"), ("distance", "min")).explain("extended")
df.filter(col("price") > 250).skylineDistinct(col("price").smin, col("distance").smin).show
df.filter(col("price") > 250).skylineDistinct(col("price").smin, col("distance").smin).explain("extended")

# advanced skyline query including a join and table aliases
val df = spark.read.option("header", true).csv("<path>/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val ratings = spark.read.option("header", true).csv("<path>/hotels_bahamas_ratings_min.csv")
ratings.registerTempTable("ratings")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM hotels h JOIN ratings r ON (h.id = r.hotel) SKYLINE OF h.price MIN, h.distance MIN").show
sqlcontext.sql("SELECT * FROM hotels h JOIN ratings r ON (h.id = r.hotel) SKYLINE OF h.price MIN, h.distance MIN").explain("extended")

# advanced skylines including HAVING
val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("<path>/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val ratings = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("<path>/hotels_bahamas_ratings_min.csv")
ratings.registerTempTable("ratings")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT h.id FROM hotels h GROUP BY h.id, h.distance HAVING min(price) > 200 ORDER BY h.id DESC, h.distance DESC").show
sqlcontext.sql("SELECT h.id FROM hotels h GROUP BY h.id, h.distance HAVING min(price) > 200 ORDER BY h.id DESC, h.distance DESC").explain("extended")
sqlcontext.sql("SELECT h.id FROM hotels h GROUP BY h.id, h.distance HAVING min(price) > 200 SKYLINE OF h.id MIN, h.distance MIN, min(h.price) MIN").show
sqlcontext.sql("SELECT h.id FROM hotels h GROUP BY h.id, h.distance HAVING min(price) > 200 SKYLINE OF h.id MIN, h.distance MIN, min(h.price) MIN").explain("extended")
sqlcontext.sql("SELECT h.id, h.distance, min(h.price) FROM hotels h GROUP BY h.id, h.distance SKYLINE OF h.id MIN, h.distance MIN, min(h.price) MIN").show
sqlcontext.sql("SELECT h.id, h.distance, min(h.price) FROM hotels h GROUP BY h.id, h.distance SKYLINE OF h.id MIN, h.distance MIN, min(h.price) MIN").explain("extended")

# TEST ALL
val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("<path>/hotels_bahamas_min.csv")
df.registerTempTable("hotels")
val ratings = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("<path>/hotels_bahamas_ratings_min.csv")
ratings.registerTempTable("ratings")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM hotels").show
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").show
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").explain("extended")
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF DISTINCT price MIN ORDER BY distance DESC, price ASC").show
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF DISTINCT price MIN ORDER BY distance DESC, price ASC").explain("extended")
sqlcontext.sql("SELECT * FROM hotels SKYLINE OF price MIN, distance MIN").show
sqlcontext.sql("SELECT * FROM hotels SKYLINE OF price MIN, distance MIN").explain("extended")
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").show
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MIN ORDER BY distance DESC, price ASC").explain("extended")
sqlcontext.sql("SELECT * FROM hotels WHERE price > 200 SKYLINE OF price MAX ORDER BY distance DESC, price ASC").queryExecution.debug.codegen
df.filter(col("price") > 250).skyline(("price", "min")).show
df.filter(col("price") > 250).skyline(("price", "min")).explain("extended")
df.filter(col("price") > 250).skyline(col("price").smin).show
df.filter(col("price") > 250).skyline(col("price").smin).explain("extended")
df.filter(col("price") > 250).skylineDistinct(("price", "min")).show
df.filter(col("price") > 250).skylineDistinct(("price", "min")).explain("extended")
df.filter(col("price") > 250).skylineDistinct(col("price").smin).show
df.filter(col("price") > 250).skylineDistinct(col("price").smin).explain("extended")
df.filter(col("price") > 250).skyline(("price", "min"), ("distance", "min")).show
df.filter(col("price") > 250).skyline(("price", "min"), ("distance", "min")).explain("extended")
df.filter(col("price") > 250).skyline(col("price").smin).show
df.filter(col("price") > 250).skyline(col("price").smin).explain("extended")
df.filter(col("price") > 250).skylineDistinct(("price", "min"), ("distance", "min")).show
df.filter(col("price") > 250).skylineDistinct(("price", "min"), ("distance", "min")).explain("extended")
df.filter(col("price") > 250).skylineDistinct(col("price").smin, col("distance").smin).show
df.filter(col("price") > 250).skylineDistinct(col("price").smin, col("distance").smin).explain("extended")
sqlcontext.sql("SELECT * FROM hotels h JOIN ratings r ON (h.id = r.hotel) SKYLINE OF h.price MIN, h.distance MIN").show
sqlcontext.sql("SELECT * FROM hotels h JOIN ratings r ON (h.id = r.hotel) SKYLINE OF h.price MIN, h.distance MIN").explain("extended")
sqlcontext.sql("SELECT * FROM hotels h SKYLINE OF h.price MIN").show
sqlcontext.sql("SELECT * FROM hotels h SKYLINE OF h.price MIN").explain("extended")
sqlcontext.sql("SELECT * FROM hotels h SKYLINE OF h.price MIN, h.distance MIN").show
sqlcontext.sql("SELECT * FROM hotels h SKYLINE OF h.price MIN, h.distance MIN").explain("extended")
sqlcontext.sql("SELECT h.id, min(h.distance) FROM hotels h GROUP BY h.id").show
sqlcontext.sql("SELECT h.id FROM hotels h GROUP BY h.id, h.distance HAVING min(price) > 200 ORDER BY h.id DESC, h.distance DESC").show
sqlcontext.sql("SELECT h.id FROM hotels h GROUP BY h.id, h.distance HAVING min(price) > 200 ORDER BY h.id DESC, h.distance DESC").explain("extended")
sqlcontext.sql("SELECT h.id FROM hotels h GROUP BY h.id, h.distance HAVING min(price) > 200 SKYLINE OF h.id MIN, h.distance MIN, min(h.price) MIN").show
sqlcontext.sql("SELECT h.id FROM hotels h GROUP BY h.id, h.distance HAVING min(price) > 200 SKYLINE OF h.id MIN, h.distance MIN, min(h.price) MIN").explain("extended")
sqlcontext.sql("SELECT h.id, h.distance, min(h.price) FROM hotels h GROUP BY h.id, h.distance SKYLINE OF h.id MIN, h.distance MIN, min(h.price) MIN").show
sqlcontext.sql("SELECT h.id, h.distance, min(h.price) FROM hotels h GROUP BY h.id, h.distance SKYLINE OF h.id MIN, h.distance MIN, min(h.price) MIN").explain("extended")
sqlcontext.sql("SELECT h.id FROM hotels h GROUP BY h.id, h.distance HAVING min(price) > 200 SKYLINE OF h.id MIN, h.distance MIN, min(h.price) MIN ORDER BY h.distance, min(h.price)").show
sqlcontext.sql("SELECT h.id FROM hotels h GROUP BY h.id, h.distance HAVING min(price) > 200 SKYLINE OF h.id MIN, h.distance MIN, min(h.price) MIN ORDER BY h.distance, min(h.price)").explain("extended")
