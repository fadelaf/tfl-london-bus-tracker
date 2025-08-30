import requests
import threading
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import time

"""import kafka producers function from producers.py"""
from producers import source_producer
from dotenv import load_dotenv
import os

load_dotenv()

""" input API ID and API KEY intu API url"""
app_id = os.getenv("API_ID")
app_key = os.getenv("API_KEY")
api_url = f"https://api.tfl.gov.uk/Line/12/Arrivals?app_id={app_id}&app_key={app_key}"

""" bus lines """
line_numbers = [12, 8, 73, 38, 25]

""" Function for fetching data from API"""
def dataFetching(line_num):

    """ declare API url """
    api_url = f"https://api.tfl.gov.uk/Line/{line_num}/Arrivals?app_id={app_id}&app_key={app_key}"
    try:
        """ request GET data from API url"""
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"thread {line_num}")
        return data # return data
    except requests.exceptions.RequestException as e:
        print(f"An Error has occured: {e}")
        logger.error(f"Error fetching data from TFL API: {e}") 
        return None # return None if error

""" function for running data fetching and send it to kafka by producer"""
def worker(line_num):
    data = dataFetching(line_num)

    """ producers function from file producers.py"""
    source_producer(data)

""" function to run multitrheading data fetching """
def main():

    while True:

        try:
            print("Get new data....")

            """ collect the thread here """
            threads = []
            for i in line_numbers:

                """declare and start the multithreading data fetching"""
                thread = threading.Thread(target=worker, args=(i,))
                thread.start() 
                threads.append(thread)

            """ after running the threads, do join() for waiting others threads stop 
            to continue into next batch"""
            for t in threads:
                t.join()

            """ get data for every 30 seconds """
            time.sleep(30)
        except Exception as e :
            print(f"ERROR : {e}")
            time.sleep(30)


if __name__== "__main__":
    
    main()