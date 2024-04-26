import pandas as pd

# 1. Create DataFrame from CSV
path = '../dataset/sample.csv'
df = pd .read_csv(path, sep=',')

# 2. Rename all the columns with snake_case format.
df = df.rename(columns={'VendorID':'Vendor_ID', 'RatecodeID':'Ratecode_ID',
                        'PULocationID':'PU_Location_ID','DOLocationID':'DO_Location_ID'})

# 3. Select only 10 top of highest number of passenger_count, show only columns vendor_id, passenger_count, trip_distance, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge from the DataFrame.
#  Select top 10 rows with the highest number of passenger_count
top_10_passenger_count = df.nlargest(10, 'passenger_count')
# Filter out the desired columns
select_culumns = ['Vendor_ID', 'passenger_count', 'trip_distance', 'payment_type', 
                    'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
                    'improvement_surcharge', 'total_amount', 'congestion_surcharge']
df = top_10_passenger_count[select_culumns]


# 4. [Extra] Cast the data type to the appropriate value.
# check Data type
print(df.dtypes)
# Cast the data types to appropriate values
df['tolls_amount'] = df['tolls_amount'].astype(float)



# print(df.columns)