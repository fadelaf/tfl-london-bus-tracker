# dashboard.py
import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
from datetime import datetime
import time
import contextlib
import pytz
import os
from dotenv import load_dotenv

load_dotenv()

# Dapatkan waktu London
london_tz = pytz.timezone('Europe/London')
london_time = datetime.now(london_tz).strftime("%H:%M:%S")

POSTGRES_HOST = os.getenv("DB_HOST")
POSTGRES_DB =  os.getenv("DB_MAIN")
POSTGRES_USER =  os.getenv("DB_USER")
POSTGRES_PASSWORD = os.getenv("DB_PASS")


# Page configuration
st.set_page_config(
    page_title="LONDON BUS LIVE TRACKER",
    page_icon="🚌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for aesthetic design
st.markdown("""
<style>
    .main {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    .stApp {
        background: linear-gradient(120deg, #fdfbfb 0%, #ebedee 100%);
        font-family: 'Inter', sans-serif;
    }
    .header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        text-align: center;
        padding: 2rem;
        border-radius: 20px;
        margin-bottom: 2rem;
        box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
    }
    .header h1 {
        font-weight: 700;
        font-size: 2.5rem;
        margin-bottom: 0.5rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
    }
    .header p {
        font-size: 1.1rem;
        opacity: 0.9;
        font-weight: 300;
    }
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 16px;
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.08);
        border-left: 4px solid #667eea;
        text-align: center;
        transition: transform 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 35px rgba(0, 0, 0, 0.15);
    }
    .data-table {
        background: white;
        border-radius: 16px;
        padding: 1.5rem;
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.08);
        border: 1px solid #eaeaea;
    }
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #2c3e50 0%, #34495e 100%);
        color: white;
        padding: 2rem 1rem;
    }
    .refresh-button {
        background: linear-gradient(45deg, #667eea, #764ba2);
        color: white;
        border: none;
        border-radius: 25px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
        transition: all 0.3s ease;
    }
    .refresh-button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
    }
    /* Custom dataframe styling */
    .dataframe {
        border-radius: 12px;
        overflow: hidden;
    }
    .dataframe thead th {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        color: white !important;
        font-weight: 600;
    }
    /* Container styling */
    .stContainer {
        border-radius: 20px;
        padding: 2rem;
        background: white;
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1);
        margin-bottom: 2rem;
    }
    .center-container {
            display: flex;
            justify-content: center;
            margin: 0 auto;
    }

</style>



""", unsafe_allow_html=True)



# Database connection
@contextlib.contextmanager
def get_db_connection():
    """Context manager untuk handle connection automatically"""
    conn = psycopg2.connect(
            host=POSTGRES_HOST, 
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port="5432"
    )
    try:
        yield conn
    finally:
        conn.close()

conn = get_db_connection()

def get_realtime_bus_data(line, conn):
    """Get real-time bus data from PostgreSQL"""
    try:
    
        query = """
                    with sortLine AS (
                    SELECT vehicleId, lineName, stationName, towards, destinationName,
                        timeToStation, 
                        expectedArrival at TIME zone 'UTC' at TIME zone 'Europe/London' as expectedArrival , 
                        timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London' as timestamp_fetch, 
                        timetolive AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London' as timeToLive
                    FROM bus_arrivals_track
                    order by timestamp_fetch desc),
                        partitionsRoute as (
                    select vehicleid, linename, stationname,
                    case 
                        when towards = 'null' then '-'
                        else towards
                    end as towards, 
                    destinationname, timetostation, expectedArrival, timetolive , timestamp_fetch,
                    row_number() over (partition by vehicleid, stationname order by timestamp_fetch desc) as row_num
                    from sortLine 
                    where linename = %s
                    and timetolive > NOW() AT TIME ZONE 'Europe/London'
                    and timeToStation  < 5)
                    select linename, vehicleid, stationname, timetostation, towards, destinationname, expectedArrival 
                    from partitionsRoute
                    where row_num = 1;
                        
                        """  
        df = pd.read_sql(query, conn, params=(line,))
        return df
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return pd.DataFrame()



 

content_placeholder = st.empty()
refresh_interval= 15


while True:
    with content_placeholder.container():

        # Header Section
        st.markdown("""
        <div class="header">
            <h1>🚌 LONDON BUS LIVE TRACKER</h1>
            <p>Real-time monitoring of bus arrivals across London</p>
            <p>Bus line: 8, 12, 25, 38, 73 </p>
            <p>🔄 | sLast update: {}</p>
        </div>
        """.format(london_time), unsafe_allow_html=True)  
 
        
        # Data Section
        with get_db_connection() as conn:
            with st.container(border=True, width=1500):
                for line in ["8", "12", "25", "38", "73"]:
                    df = get_realtime_bus_data(line, conn)
                    st.markdown(f"""
                        <div style="
                            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
                            color: white;
                            padding: 1rem;
                            border-radius: 15px;
                            text-align: center;
                            margin: 1rem 0;
                            font-size: 1.6rem;
                            font-weight: bold;
                            box-shadow: 0 6px 20px rgba(79, 172, 254, 0.25);
                        ">
                        🚌 Bus in Line: {line}
                        <p>Time to station in minutes</p>
                        </div>
                        """, unsafe_allow_html=True)
                    st.dataframe( df[['linename', 'vehicleid', 'stationname', 'timetostation', 'expectedarrival', 
                                      'towards', 'destinationname' ]],
                                    column_config={
                                        "linename": "Bus Line",
                                        "vehicleid": "Bus ID",
                                        "stationname": "Next Stop", 
                                        "expectedarrival":"Expected Arrival Time",
                                        "timetostation": "Arriving in",
                                        "towards": "Direction",
                                        "destinationname":"Destination",
                                    }, 
                                    use_container_width=True, 
                                    height=550)
    
    # Wait for next refresh
    time.sleep(refresh_interval)
    
    # Clear placeholder untuk refresh smooth
    content_placeholder.empty()