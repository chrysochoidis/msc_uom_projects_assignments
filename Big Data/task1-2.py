#imports
from pyspark.sql.functions import regexp_replace, upper, count, sum, round, col
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession

#Initialize spark session
spark = SparkSession.builder \
		.appName("mai25067") \
		.master("local[*]") \
		.getOrCreate()
		
#save the given input file in a variable
file = spark.read.csv('file:///home/bigdata/customer_shopping_data.csv', header=True, inferSchema=True)
#file.show()

#-------------------Task 1--------------------------

#Task 1.1 replace " " with "_" in cols payment_method & shopping_mall
file1_1 = file.withColumn('payment_method', regexp_replace('payment_method',' ', '_')).withColumn('shopping_mall', regexp_replace('shopping_mall', ' ', '_'))
print("Task 1.1 done")

#Task 1.2 make col shopping_mall Capitals
file1_2 = file1_1.withColumn('shopping_mall', upper('shopping_mall'))
print("Task 1.2 done")

#Task 1.3 save to variable the total transactions in file
file1_3 = file1_2.count()
#print("Total transactions : " + str(file1_3))
print("Task 1.3 done")

#Task 1.4 convert column 'price' so that it shows the price in EUR and not in TL
file1_4 = file1_2.withColumn("price", round(file1_2.price * 0.1))
print("Task 1.4 done")

#Task 1.5 create a variable that has the amount of transactions by quantity and then export it to csv
file1_5 = file1_4.groupBy('quantity').count()
file1_5.write.csv('file:///home/bigdata/mai25067/transactions_by_quantity',header=True, mode='overwrite')
print("Task 1.5 done")

#Task 1.6 create a variable that has the total amount in each transaction and then export it to csv
file1_6a = file1_4.withColumn('total', file1_4.price * file1_4.quantity)
file1_6b = file1_6a.select('invoice_no', 'total')
file1_6b.write.csv('file:///home/bigdata/mai25067/total_price_by_transaction',header=True, mode='overwrite')
print("Task 1.6 done")
print("----------Task 1 done----------")

#-------------------Task 2--------------------------
#Create a statistics table for all transactions by mall
#table will have columns : shopping_mall, total_money_spent, sold_products_amount, transactions_amount
file2 = file1_6a


#Task 2.1
#create df with malls and the sum of total money spent in each mall. total money is the sum of column total (price * quantity as task 1.4) for each mall. Round and sort alphabeticaly
totalMoneySpent = file2.groupBy('shopping_mall').agg(sum('total').alias('total_money_spent_EUR'))
totalMoneySpent = totalMoneySpent.withColumn("total_money_spent_EUR", round(totalMoneySpent.total_money_spent_EUR))
totalMoneySpent = totalMoneySpent.orderBy("shopping_mall", ascending=True)
print("Task 2.1 done")

#Task 2.2
#create df with malls and the sum of quantity for each mall. Sort alphabetically
totalQuantity = file2.groupBy('shopping_mall').agg(sum('quantity').alias('sold_products_amount'))
totalQuantity = totalQuantity.orderBy("shopping_mall", ascending=True)
print("Task 2.2 done")

#Task 2.3
#create df with malls and the amount of transactions made in each mall. Sort alphabetically
totalTransactions = file2.groupBy('shopping_mall').count().withColumnRenamed('count', 'total_transactions')
totalTransactions = totalTransactions.orderBy("shopping_mall", ascending=True)

statsTable = totalMoneySpent.join(totalQuantity,['shopping_mall']).join(totalTransactions,['shopping_mall'])

#export statsTable to csv
statsTable.write.csv('file:///home/bigdata/mai25067/statistics_table',header=True, mode='overwrite')
print("Task 2.3 done")
print("----------Task 2 done----------")