// File with examples to be used in spark-shell (copy individually as needed)
// TODO: replace <path> with the (absolute) path to the .csv files

val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("<path>/simple.csv")
df.registerTempTable("simple")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM simple").show
sqlcontext.sql("SELECT * FROM simple SKYLINE OF b MAX, c MAX").show

val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("<path>/cyclic_dominance.csv")
df.registerTempTable("cyclicdominance")
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
sqlcontext.sql("SELECT * FROM cyclicdominance").show
sqlcontext.sql("SELECT * FROM cyclicdominance SKYLINE OF b MIN, c MIN, d MIN").show
