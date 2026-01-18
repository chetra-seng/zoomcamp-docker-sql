#!/usr/bin/env python
# coding: utf-8

# In[6]:


import pandas as pd


# In[8]:


prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'


# In[9]:


url = f'{prefix}/yellow/yellow_tripdata_2021-01.csv.gz'


# In[11]:


df = pd.read_csv(url)


# In[18]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    url,
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)


# In[19]:


df


# In[23]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[26]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[38]:


df_iter = pd.read_csv(url, dtype=dtype, iterator=True, chunksize=100000)


# In[ ]:


for df_chunk in df_iter:
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[ ]:





# In[ ]:




