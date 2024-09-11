#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import pandas as pd

from sqlalchemy import create_engine


# In[2]:


MAX_RETRIES = 5
SLEEP_TIME = 1


# In[3]:


retry = 1
while retry <= MAX_RETRIES:
    try:
        engine = create_engine("postgresql://root:wordpass@database:5432/covid")
        engine.connect()
        break
    except:
        if retry == MAX_RETRIES:
            raise Exception("Database connection failed.")
        retry += 1
        time.sleep(SLEEP_TIME)


# In[4]:


df = pd.read_csv("../data/owid-covid-data.csv")
df_deaths = df[["location", "date", "total_cases", "new_cases", "total_deaths", "population"]]
df_deaths.to_sql(con=engine, name="deaths")

