from kafka import KafkaConsumer
import json
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

""" declare kafka bootstrap server and postrgesql environment"""
bootstrap_servers = [os.getenv("BOOTSTRAP_SERVERS")]
POSTGRES_HOST = os.getenv("DB_HOST")
POSTGRES_DB =  os.getenv("DB_MAIN")
POSTGRES_USER =  os.getenv("DB_USER")
POSTGRES_PASSWORD = os.getenv("DB_PASS")


""" topic for sending raw data into Transformation process """
topicName_API = "tfl.source.data_API"


""" topic for sending cleaned data into consumer 
    for inserting data to postgres"""
topicName_Clean = "tfl.source.data_bus"


"""declare consumer kafka"""
def consume(topicName):
    consumer = KafkaConsumer(topicName, auto_offset_reset='latest', 
                            bootstrap_servers = bootstrap_servers,
                            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
    return consumer

""" function to consume data with Kafka from producers which is sent raw data"""
def consume_source():

    """receive/consume raw data from kafka producers"""
    data_consume = consume(topicName_API)
    
    data_store = []

    """ store data into list of dictionary"""
    for i in data_consume:
        data_store = i.value # get the value from producers
        break

    return data_store

""" function to receive/consume data with Kafka from pyspark streaming which is sent clean data"""
def postgre_consumer():

    """receive/consume raw data from kafka producers"""
    data_consume = consume(topicName_Clean)

    """ create connection to postgresql"""
    conn = psycopg2.connect(
    host=POSTGRES_HOST, 
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    port="5432"
)   
    
    cursor= conn.cursor()
    print("Connect to tfl_data database Success")

    """ insert streming data to postgresql"""
    for msg in data_consume:
        bus_data = msg.value

        try:
            cursor.execute("""
                                INSERT INTO bus_arrivals_track
                                ( vehicleId, lineName, stationName, destinationName, timeToStation, expectedArrival, timestamp, towards, timeToLive )
                        
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s )
                                ON CONFLICT DO NOTHING
                        """, (
                            
                            bus_data['vehicleId'],
                            bus_data['lineName'],
                            bus_data['stationName'],
                            bus_data['destinationName'],
                            bus_data['timeToStation'],
                            bus_data['expectedArrival'],
                            bus_data['timestamp'],
                            bus_data['towards'],
                            bus_data['timeToLive']
                            
                        ))
           
            print(f"Data has been inserted with for vehicleId: {bus_data['vehicleId']} and line {bus_data['lineName']}")
            
        except Exception as e:
            print(f"Error connection to tfl_data: {e}")
        
    conn.commit()

    """close connection after finish"""
    conn.close()


if __name__ == "__main__":
    postgre_consumer()