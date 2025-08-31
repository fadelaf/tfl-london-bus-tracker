from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType,StructField, StructType, StringType, IntegerType, TimestampType
import pyspark.sql.functions as sf

from dotenv import load_dotenv
import os

load_dotenv()

""" declare kafka bootstrap server """
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")

""" topic for sending raw data into Transformation process """
topicName_API = "tfl.source.data_API"

""" topic for sending cleaned data into consumer 
    for inserting data to postgres"""
topicName_Clean = "tfl.source.data_bus"

""" spark builder for local(development) """
# for development only
## you may set your own configuration for pypspark
os.environ["JAVA_HOME"] = os.getenv("JAVA_HOME_PATH")
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
spark = SparkSession.builder \
        .appName("LondonBusTracker") \
        .config("spark.jars", os.getenv("SPARK_CONFIG_LOCAL"))\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()


""" spark builder for production """
# for production 
# spark = SparkSession.builder \
#         .appName("LondonBusTracker") \
#         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3") \
#         .getOrCreate()

""" create data schema """
schema = StructType([
            StructField("vehicleId",StringType(),True),
            StructField("lineName",StringType(),True),
            StructField("stationName",StringType(),True),
            StructField("destinationName",StringType(),True),
            StructField("timeToStation",IntegerType(),True),
            StructField("expectedArrival",StringType(),True),
            StructField("timestamp",StringType(),True),
            StructField("towards",StringType(),True),
            StructField("timeToLive",StringType(),True)])

""" stream consuming data from kafka producers """
data_stream = spark.readStream \
                .format("kafka")\
                .option("kafka.bootstrap.servers", bootstrap_servers)\
                .option("subscribe", topicName_API ) \
                .option("startingOffsets", "latest") \
                .load()



""" get data from readStream and convert it into pyspark dataframe following the schema """
def get_data():

    parsed_stream = data_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("json_arr", sf.from_json(sf.col("json_str"), sf.ArrayType(schema))) \
    .withColumn("bus", sf.explode(sf.col("json_arr"))) \
    .select("bus.*")

    return parsed_stream

""" transforming data """
def data_transform():

    try:
        """ pick data """
        bus_track = get_data()

        """ replace(delete) "Z" for zulu time format into none"""            
        bus_track = bus_track.withColumn("expectedArrival", sf.regexp_replace("expectedArrival","Z",""))\
                            .withColumn("timeToLive", sf.regexp_replace("timeToLive","Z",""))\
                            .withColumn("timestamp", sf.regexp_replace("timestamp","Z",""))
                    
        """ convert to timestamp """         
        bus_track = bus_track.withColumn("expectedArrival", sf.to_timestamp("expectedArrival"))\
                            .withColumn("timeToLive", sf.to_timestamp("timeToLive"))\
                            .withColumn("timestamp", sf.to_timestamp("timestamp"))

        """ convert from second to minute for every data in column timeToStation """
        bus_track = bus_track.withColumn("timeToStation", (sf.col("timeToStation")/60).cast("int"))

        print("data transformation success")
        return bus_track

    except Exception as e:
        # return "Error"
        print(f"error {e}")

""" function to stream data """
def streamData():

    """ WriteStream for continue stream and sending data into another Kafka consumer """

    try:
        bus_track = data_transform()
        query = bus_track.selectExpr("to_json(struct(*)) AS value") \
                    .writeStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", bootstrap_servers)\
                    .option("topic", "tfl.source.data_bus") \
                    .option("checkpointLocation", "/tmp/spark-checkpoints")\
                    .start()
        query.awaitTermination()

    except Exception as e:   
        print(f"{e}")


if __name__ == "__main__":

    streamData()


