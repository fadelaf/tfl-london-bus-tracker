import psycopg2
import time
import os
from dotenv import load_dotenv

load_dotenv()

""" declare postgre environment"""
POSTGRES_HOST = os.getenv("DB_HOST")
POSTGRES_DB =  os.getenv("DB_MAIN")
POSTGRES_USER =  os.getenv("DB_USER")
POSTGRES_PASSWORD = os.getenv("DB_PASS")

""" function to delete expired data in postgresql """
def deleteExpiredData():
    try:
        """ create connection to postgre sql """
        conn = psycopg2.connect(
            host=POSTGRES_HOST, 
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port="5432"
        )

        print("Connect to tfl_data database Success")

        """ execute delete expired data """
        cursor= conn.cursor()
        cursor.execute("""

                        DELETE FROM bus_arrivals_track WHERE timetolive AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London' < NOW() AT TIME ZONE 'Europe/London' ;

                    """)
        conn.commit()
        print(f"Expired data has been deleted")
        
    except Exception as e:
        print(f"Error connection to tfl_data: {e}")

    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Connection Closed")

""" function to run expired data deletion """
def delete_run():

    while True:

        try:
            time.sleep(300)
            deleteExpiredData()

        except Exception as e:
            print(f"Error deleteiion: {e}")



if __name__ == "__main__":

    delete_run()

