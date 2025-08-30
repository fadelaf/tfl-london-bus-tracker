from kafka import KafkaConsumer
import json
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

bootstrap_servers = [os.getenv("BOOTSTRAP_SERVERS")]
POSTGRES_HOST = os.getenv("DB_HOST")
POSTGRES_DB =  os.getenv("DB_MAIN")
POSTGRES_USER =  os.getenv("DB_USER")
POSTGRES_PASSWORD = os.getenv("DB_PASS")
topicName_API = "tfl.source.data_API"
topicName_Clean = "tfl.source.data_bus"



def consume(topicName):
    consumer = KafkaConsumer(topicName, auto_offset_reset='latest', 
                            bootstrap_servers = bootstrap_servers,
                            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
    return consumer

def consume_source():

    data_consume = consume(topicName_API)

    data_store = []

    for i in data_consume:
        data_store = i.value
        break

    return data_store


def postgre_consumer():

             

    data_consume = consume(topicName_Clean)

    conn = psycopg2.connect(
    host=POSTGRES_HOST, 
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    port="5432"
)   
    
    cursor= conn.cursor()
    print("Connect to tfl_data database Success")

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
    conn.close()


if __name__ == "__main__":
    postgre_consumer()