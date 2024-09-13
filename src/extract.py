#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import random
import time

from sqlalchemy import create_engine


# In[2]:


MAX_RETRIES = 5


# In[3]:


print("establishing connection...")
retry = 1
while retry <= MAX_RETRIES:
    try:
        engine = create_engine("postgresql://root:wordpass@database:5432/space_objects")
        engine.connect()
        print("connection established")
        break
    except:
        if retry == MAX_RETRIES:
            raise Exception("Database connection failed.")
        retry += 1
        time.sleep(1)


# In[ ]:


print("reading csv into master dataframe...")
df = pd.read_csv("../data/nearest-earth-objects.csv")
print("reading done")


# In[ ]:


print("imitating real time database insertions...")
index = 0
batch_size = random.randint(1, 5)
batch_df = pd.DataFrame()
while index < len(df):
    batch_df = pd.concat([batch_df, df.iloc[[index]]])
    if len(batch_df) == batch_size:
        batch_df.to_sql(con=engine, name="objects", if_exists="append")
        print(f"inserted a batch of size {batch_size}")
        batch_size = random.randint(1, 5)
        batch_df = pd.DataFrame()
        time.sleep(5)
    index += 1
print("imitating done")

