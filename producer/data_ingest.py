import requests
import threading
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import time
from producers import source_producer
from dotenv import load_dotenv
import os

load_dotenv()

app_id = os.getenv("API_ID")
app_key = os.getenv("API_KEY")
api_url = f"https://api.tfl.gov.uk/Line/12/Arrivals?app_id={app_id}&app_key={app_key}"

    
line_numbers = [12, 8, 73, 38, 25]

def dataFetching(line_num):
    api_url = f"https://api.tfl.gov.uk/Line/{line_num}/Arrivals?app_id={app_id}&app_key={app_key}"
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"thread {line_num}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"An Error has occured: {e}")
        logger.error(f"Error fetching data from TFL API: {e}")
        return None
    
def worker(line_num):
    data = dataFetching(line_num)
    source_producer(data)

def main():
    while True:

        try:
            print("Get new data....")
            threads = []
            for i in line_numbers:

                thread = threading.Thread(target=worker, args=(i,))
                thread.start() 
                threads.append(thread)

            for t in threads:
                t.join()

            time.sleep(30)
        except Exception as e :
            print(f"ERROR : {e}")
            time.sleep(30)


if __name__== "__main__":

    main()