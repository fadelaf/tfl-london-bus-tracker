import requests
import time
from kafka import KafkaProducer

app_id = 'ef770f2eae904782ba413629cc958420'
app_key = '3d01a46d255048c0acb9fc8aff20ec70'

api_url = f"https://api.tfl.gov.uk/Line/12/Arrivals?app_id={app_id}&app_key={app_key}"

def dataFetching():
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"An Error has occured: {e}")


bootstrap_servers = ['localhost:29092']
topicName = "testing"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                        value_serializer=lambda v: str(v).encode('utf-8')  # Auto encode
                        )

producer.send(topicName, dataFetching())
producer.flush()
print("Messages sent successfully!")



# try:
#     while True:
#         dataFetching()
#         print("waiting for next fetch in 30 seconds....")
#         time.sleep(30)

# except KeyboardInterrupt:
#     print("Program stopped")
