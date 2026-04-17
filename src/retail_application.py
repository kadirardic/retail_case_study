from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName("Spark Retail Case Study")
             .master("local")
             .config("spark.jars","../_resources/lib/mysql-connector-java-8.0.28.jar")
             .getOrCreate())


    mysql_jdbc_properties = {
        "url" : "jdbc:mysql://localhost:3306/retaildb",
        "user": "root",
        "password": "qwerty",
        "dbtable" : "products",
        "driver":"com.mysql.cj.jdbc.Driver"
    }

    products_df = spark.read.format("jdbc").options(**mysql_jdbc_properties).load()

    mysql_jdbc_properties["dbtable"] = "customers"

    customers_df = spark.read.format("jdbc").options(**mysql_jdbc_properties).load()

    ORDER_SCHEMA = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", StringType()),
        StructField("order_time", StringType()),
    ])

    input_df = spark.read.json(path="../_resources/orders/landing/orders_batch_01.json",
                               schema=ORDER_SCHEMA
                              )

    result_df= input_df.join(products_df, "product_id").join(customers_df, "customer_id")

    order_details_df =  result_df.select("order_id", "customer_name", "city", "product_name", "quantity", "price", (col( "price") * col("quantity")).alias("total_price"))

    mysql_jdbc_properties["dbtable"] = "order_details"

    order_details_df.show()

    order_details_df.write.options(**mysql_jdbc_properties).save(format="jdbc", mode="overwrite")



