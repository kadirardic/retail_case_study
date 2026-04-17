from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__ == '__main__':
    libraries = "../_resources/lib/mysql-connector-java-8.0.28.jar, ../_resources/lib/commons-pool2-2.11.1.jar, ../_resources/lib/kafka-clients-2.1.1.jar, ../_resources/lib/spark-sql-kafka-0-10_2.12-3.2.1.jar, ../_resources/lib/spark-token-provider-kafka-0-10_2.12-3.4.1.jar"

    spark = (SparkSession
             .builder
             .appName("Spark Retail Case Study")
             .master("local")
             .config("spark.jars", libraries)
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

    #input_df = (spark.readStream
    #            .option("kafka.bootstrap.servers", "localhost:9092")
    #            .option("subscribe", "my-topic-01")
    #            .load(format="kafka"))

    input_df = (spark.read
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "my-topic-01")
                .load(format="kafka"))

    stream_df = input_df.select(
        col("value").cast(StringType()),
        split(col("value").cast(StringType()), ",").getItem(0).alias("order_id"),
        split(col("value").cast(StringType()), ",").getItem(1).alias("customer_id"),
        split(col("value").cast(StringType()), ",").getItem(2).alias("product_id"),
        split(col("value").cast(StringType()), ",").getItem(3).alias("quantity"),
        split(col("value").cast(StringType()), ",").getItem(4).alias("order_time"),
    )

    #stream_df.writeStream.start(format="console", outputMode="APPEND").awaitTermination()

    result_df= stream_df.join(products_df, "product_id").join(customers_df, "customer_id")

    order_details_df =  result_df.select("order_id", "customer_name", "city", "product_name", "quantity", "price", (col( "price") * col("quantity")).alias("total_price"))

    mysql_jdbc_properties["dbtable"] = "order_details"

    order_details_df.show()

    order_details_df.write.options(**mysql_jdbc_properties).save(format="jdbc", mode="overwrite")


