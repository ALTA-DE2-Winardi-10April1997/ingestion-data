import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import BigInteger, DateTime, Float, Boolean, Integer

# 2. Load the parquet file to a DataFrame with fastparquet library.
def get_path(): 
    parquet_path = '../dataset/yellow_tripdata_2023-01.parquet'
    return parquet_path

def get_dataframe(path):
    df  = pd.read_parquet(path, engine='fastparquet')
    return df

# 3.Clean the Yellow Trip dataset.
def clean_data(df):
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].replace({'Y': False, 'N': True})
    return df

def get_postgres_connection():
    user = 'postgres'
    password = 'admin'
    host = 'localhost'
    database = 'mydb'
    port =5432

    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn_string)

    return engine

# 4. Define the data type schema when using to_sql method.
def load_to_postgres(conn, clean_df):
    df_schema = {
        "VendorID": BigInteger,
        "tpep_pickup_datetime": DateTime,
        "tpep_dropoff_datetime": DateTime,
        "passenger_count": BigInteger,
        "trip_distance": Float,
        "RatecodeID": Float,
        "store_and_fwd_flag": Boolean,
        "PULocationID": Integer,
        "DOLocationID": Integer,
        "payment_type": Integer,
        "fare_amount": Float,
        "extra": Float,
        "mta_tax": Float,
        "tip_amount": Float,
        "tolls_amount": Float,
        "improvement_surcharge": Float,
        "total_amount": Float,
        "congestion_surcharge": Float,
        "airport_fee": Float
    }

    clean_df.to_sql(name='tugas_part2', con=conn, if_exists='replace', schema='public', index=False, dtype=df_schema )




# 5. Ingest the Yellow Trip dataset to PostgreSQL
path = get_path()
df = get_dataframe(path)
clean_df = clean_data(df)
conn = get_postgres_connection()
load_to_postgres(conn, clean_df)
print('data berhasil di ingest')