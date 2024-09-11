#!/usr/bin/env python
# coding: utf-8

# In[1]:


import random
import time

import pandas as pd

from sqlalchemy import create_engine


# In[2]:


MAX_RETRIES = 5


# In[3]:


print("establishing connection...")
retry = 1
while retry <= MAX_RETRIES:
    try:
        engine = create_engine("postgresql://root:wordpass@database:5432/covid")
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
df = pd.read_csv("../data/owid-covid-data.csv")
df = df[["continent", "location", "date", "new_cases", "total_cases", "new_deaths", "total_deaths", "reproduction_rate", "population", "new_vaccinations", "total_vaccinations", "median_age", "aged_65_older", "aged_70_older", "life_expectancy"]]
df = df.sort_values(by="date")
print("reading complete")


# In[ ]:


print("imitating database insertions...")
index = 0
batch_size = random.randint(1, 5)
batch_df = pd.DataFrame()
while index < len(df):
    batch_df = pd.concat([batch_df, df.iloc[[index]]])
    if len(batch_df) == batch_size:
        batch_df.to_sql(con=engine, name="deaths", if_exists="append")
        print(f"inserted a batch of size {batch_size}")
        batch_size = random.randint(1, 5)
        batch_df = pd.DataFrame()
        time.sleep(5)
    index += 1
print("imitation complete")

