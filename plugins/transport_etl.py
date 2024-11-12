import pandas as pd
import json
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from airflow.hooks.base import BaseHook
from transport_utils import setup_logger, fetch_data
import io




# Set up logger
logger = setup_logger('dublin_transport')

# Extract functions
def extract_luas_data(url):
    """Extract Luas passenger data"""
    try:
        logger.info("Starting Luas data extraction")
        content = fetch_data(url)
        # Convert content to DataFrame
        df = pd.read_csv(io.StringIO(content.decode('utf-8')))
        # Add creation timestamp in a format that can be serialized
        df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Successfully extracted {len(df)} rows of Luas data")
        logger.info(f"Date range in data: from {df['TLIST(A1)'].min()} to {df['TLIST(A1)'].max()}")
        
        return df
    except Exception as e:
        logger.error(f"Error extracting Luas data: {str(e)}")
        raise



def extract_weather_data(url):
    """Extract weather data"""
    try:
        logger.info("Starting weather data extraction")
        content = fetch_data(url)
        
        # Attempt to read CSV with flexible parsing options
        df = pd.read_csv(
            io.StringIO(content.decode('utf-8')),
            skiprows=13,       # Adjust this based on metadata rows
            sep=",",           # Try comma separator
            on_bad_lines='skip' # Skip problematic lines
        )
        
        # Log initial rows and column count for inspection
        logger.info(f"Initial extracted DataFrame:\n{df.head()}")
        logger.info(f"DataFrame shape: {df.shape}")
        
        # Define expected column names with all "ind" columns enumerated
        if df.shape[1] == 9:  # Check if there are 9 columns after skipping
            df.columns = ['DATE', 'IND1', 'RAINFALL_MM', 'IND2', 'MAXT', 'IND3', 'MINT', 'GMIN', 'SOIL']
            # Keep all columns as per request
            df['CREATED_AT'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        else:
            logger.error("Unexpected number of columns. Data structure may have changed.")
            raise ValueError("Data does not have the expected number of columns.")
        
        logger.info(f"Successfully extracted {len(df)} rows of weather data")
        return df
    except Exception as e:
        logger.error(f"Error extracting weather data: {str(e)}")
        raise

def extract_bikes_data(url):
    """Extract Dublin Bikes data"""
    try:
        logger.info("Starting Dublin Bikes data extraction")
        content = fetch_data(url)
        data = json.loads(content)
        df = pd.DataFrame(data)
        
        # Convert timestamp to string format
        df['last_reported'] = pd.to_datetime(df['last_reported']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['CREATED_AT'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"Successfully extracted {len(df)} rows of Dublin Bikes data")
        return df
    except Exception as e:
        logger.error(f"Error extracting Dublin Bikes data: {str(e)}")
        raise

def extract_cycle_counts_data(url):
    """Extract Cycle Counts data"""
    try:
        logger.info("Starting Cycle Counts data extraction")
        content = fetch_data(url)
        df = pd.read_csv(io.StringIO(content.decode('utf-8')))
        
        # Melt the DataFrame to convert columns to rows
        id_vars = ['Time']
        melted_df = pd.melt(
            df, 
            id_vars=id_vars,
            var_name='location_full',
            value_name='count'
        )
        
        # Clean and split the location column
        melted_df['location'] = melted_df['location_full'].str.split('Cyclist').str[0].str.strip()
        melted_df['direction'] = melted_df['location_full'].str.extract(r'Cyclist\s*(.*?)$').fillna('')
        
        # Create final DataFrame
        result_df = pd.DataFrame({
            'TIMESTAMP': pd.to_datetime(melted_df['Time'], format='%d/%m/%Y %H:%M').dt.strftime('%Y-%m-%d %H:%M:%S'),
            'LOCATION': melted_df['location'],
            'DIRECTION': melted_df['direction'],
            'COUNT': melted_df['count'],
            'CREATED_AT': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        logger.info(f"Successfully extracted and transformed {len(result_df)} rows of Cycle Counts data")
        return result_df
    except Exception as e:
        logger.error(f"Error extracting Cycle Counts data: {str(e)}")
        raise

def extract_dublin_bus_data(url):
    """Extract Dublin Bus passenger data"""
    try:
        logger.info("Starting Dublin Bus data extraction")
        content = fetch_data(url)
        df = pd.read_csv(io.StringIO(content.decode('utf-8')))
        df['CREATED_AT'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Successfully extracted {len(df)} rows of Dublin Bus data")
        return df
    except Exception as e:
        logger.error(f"Error extracting Dublin Bus data: {str(e)}")
        raise



def load_to_snowflake(df, table_name):
    """Load DataFrame to Snowflake table."""
    try:
        logger.info(f"Starting data load to {table_name}")
        conn = BaseHook.get_connection('snowflake_default')
        
        # Convert CREATED_AT back to datetime if it's a string
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'])
        
        # Create a copy of the DataFrame to avoid modifying the original
        df_to_load = df.copy()
        
        # Rename problematic columns only if they exist
        column_mapping = {
            'TLIST(A1)': 'TLIST'
            # Add other column mappings if needed
        }
        
        # Only rename columns that exist in the DataFrame
        columns_to_rename = {k: v for k, v in column_mapping.items() if k in df_to_load.columns}
        if columns_to_rename:
            df_to_load = df_to_load.rename(columns=columns_to_rename)
        

        # Convert all column names to uppercase and replace spaces with underscores
     # Apply transformations to column names
        
        df_to_load.columns = (
            df_to_load.columns.str.upper()
            .str.replace('UNABLE TO REINSTALL REPAIRED COUNTER DUE TO ROADWORKS', '', regex=False)
            .str.replace('COUNTER REMOVED FOR ROADWORKS','', regex=False)
            .str.replace('(', '', regex=False)
            .str.replace(')', '', regex=False)
            .str.replace(' ', '_', regex=False)
        )

# Apply deduplication to column names

        # Replace empty strings or whitespace-only values with None (interpreted as NULL in Snowflake)
        df_to_load = df_to_load.replace(r'^\s*$', None, regex=True)

        snowflake_conn = snowflake.connector.connect(
            user=conn.login,
            password=conn.password,
            account=conn.extra_dejson.get('account'),
            warehouse=conn.extra_dejson.get('warehouse'),
            database='DUBLIN_TRANSPORTATION',
            schema='RAW'
        )
        
        write_pandas(
            conn=snowflake_conn,
            df=df_to_load,
            table_name=table_name,
            database='DUBLIN_TRANSPORTATION',
            schema='RAW'
        )
        
        snowflake_conn.close()
        logger.info(f"Successfully loaded {len(df_to_load)} rows into {table_name}")
    except Exception as e:
        logger.error(f"Error loading data into Snowflake: {str(e)}")
        raise
