val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("/home/lukas/spark/data/simple.csv")
df.registerTempTable("simple")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM simple").show
sqlcontext.sql("SELECT * FROM simple SKYLINE OF b MAX, c MAX").show

val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("/home/lukas/spark/data/cyclic_dominance.csv")
df.registerTempTable("cyclicdominance")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM cyclicdominance").show
sqlcontext.sql("SELECT * FROM cyclicdominance SKYLINE OF b MIN, c MIN, d MIN").show
