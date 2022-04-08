#!/usr/bin/env python
# coding: utf-8

# In[97]:


import pandas as pd
import sys

path = sys.argv[1]
columns=["IP","identifier","email","time","request","url","protocol","status_code",             "bytesize","referer","user-agent","go-agent","processing time"]
rawData = pd.read_csv(f'/home/jmyeong/tarfile/admin_log/{path}' ,sep=' ',header=None, names=columns)

rawData.reset_index(inplace = True)
del rawData["time"]
rawData.columns = columns
rawData["time"] = rawData["time"].apply(lambda x : x.replace("[",""))
rawData.to_csv(f'/home/jmyeong/tarfile/admin_log/{path}',mode='w', header=False,index= False)
print('complete')

