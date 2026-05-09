#!/usr/bin/env python
# coding: utf-8

# Practical no 4: -
# 
# Design and develop a distributed application to find the coolest/hottest year from the available weather data. Use weather data from the Internet and process it using MapReduce.

# In[1]:


import pandas as pd


# In[2]:


file_path = "dataset/GlobalLandTemperatures.csv"


# In[3]:


# Step 1: Load dataset
df = pd.read_csv(file_path)


# In[4]:


df.head()


# In[5]:


df.info()


# In[6]:


df.describe()


# In[7]:


df.isnull().sum()


# In[8]:


# Step 2: Clean data (remove missing values)
df = df.dropna(subset=["AverageTemperature"])


# In[9]:


df.isnull().sum()


# In[10]:


# Step 3: Extract year
df["Year"] = pd.to_datetime(df["dt"]).dt.year


# In[11]:


# Step 4: MapReduce logic using groupby
year_temp = df.groupby("Year")["AverageTemperature"].max()


# In[12]:


# Step 5: Find hottest & coolest year
hottest_year = year_temp.idxmax()
coolest_year = year_temp.idxmin()


# In[13]:


print("Hottest Year:", hottest_year, "Temp:", year_temp[hottest_year])
print("Coolest Year:", coolest_year, "Temp:", year_temp[coolest_year])


# Grouping operation in pandas (groupby) performs MapReduce-like aggregation, where data is mapped by year and reduced using max function.
