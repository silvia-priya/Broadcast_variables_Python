import pyspark
sc=pyspark.SparkContext('local[*]')
sc.setLogLevel('WARN')

#Building a dictionary for storing 
prod_codes = {"P101":"Cosmetics","P102":"Furniture","P103":"Handmade"}

#The original data(sequence/file) ,which can be converted into RDD . This RDD is in need of the previous map
orders = [("Customer1","O0901","P101"),("Customer2","O0902","P102"),("Customer3","O0903","P103")]

ordersRDD =sc.parallelize(orders)

#converting the map into the broadcast variable

prod_broad = sc.broadcast(prod_codes)

def prod_desc(product):
  return prod_broad.values[product]

res= ordersRDD.map(lambda x:(x[0],x[1],x[2],prod_desc(x[2])))

res.saveAsTextFile("/home/hadoop/Learnbay/Spark/python_broadcast")


