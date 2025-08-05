from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import sum, round, col, count
from pyspark.sql.functions import current_timestamp

#Task3
#Initialize spark session
spark = SparkSession.builder \
		.appName("mai25067") \
		.master("local[*]") \
		.getOrCreate()

#create the stream schema
userSchema = StructType()\
        .add("invoice_no","string")\
        .add("customer_id","string")\
        .add("gender","string")\
        .add("age","integer")\
        .add("category","string")\
        .add("quantity","integer")\
        .add("price","integer")\
        .add("payment_method","string")\
        .add("invoice_date","string")\
        .add("shopping_mall","string")

#??? watermark or timestamp probably doesnt work

csvStream = spark.readStream.option("sep", ",").option("header", "false").schema(userSchema)\
    .csv("file:///home/bigdata/mai25067_input_stream/")\
    .withColumn('total', col("price") * col("quantity"))\
    .withColumn("time", current_timestamp())\
    .withWatermark("time", "10 milliseconds")

#create the statTable to export
statTableQuery = csvStream.groupBy('shopping_mall')\
    .agg(sum('total').alias('total_money_spent_EUR'),sum('quantity').alias('sold_products_amount'),count('*').alias('total_transactions'))\
    .withColumn("total_money_spent_EUR", round(col("total_money_spent_EUR")))\
    .select(
        col("shopping_mall"),
        col("total_money_spent_EUR"),
        col("sold_products_amount"),
        col("total_transactions")
    )
 
statTableQuery.writeStream.outputMode("append").option("path","file:///home/bigdata/mai25067_output/").option("checkpointLocation", "file:///home/bigdata/mai25067_checkpoint/").format("csv").start().awaitTermination() 

# unfinished.....error with watermark??