#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd


# In[2]:


path = os.getcwd()
path = os.path.join(path,'assets')
path = os.path.join(path,'nombre_columnas_app.py')
file1= "BPP3COLS.xlsx"
file2 = "dfdesc3COLS.xlsx"
file3 = "PPC3COLS.xlsx"
file4 = "PPM3COLS.xlsx"
file5 = "SPC3COLS.xlsx"
file6 = "SPM3COLS.xlsx"
file7 = "SUBPC3COLS.xlsx"
file8 = "SUBPM3COLS.xlsx"
file9 = "BPP4COLS.xlsx"
file10 = "dfdesc4COLS.xlsx"
file11= "dfcorrlat4COLS.xlsx"
file12 = "PPC4COLS.xlsx"
file13 = "PPM4COLS.xlsx"
file14 = "SPC4COLS.xlsx"
file15 = "SPM4COLS.xlsx"
file16= "SUBPC4COLS.xlsx"
file17 = "SUBPM4COLS.xlsx"


# In[3]:


ff1 =os.path.join(path, file1)
ff2 = os.path.join(path, file2)
ff3 = os.path.join(path, file3)
ff4 = os.path.join(path, file4)
ff5 = os.path.join(path, file5)
ff6 = os.path.join(path, file6)
ff7 = os.path.join(path, file7)
ff8 = os.path.join(path, file8)
ff9 = os.path.join(path, file9)
ff10 = os.path.join(path, file10)
ff11 = os.path.join(path, file11)
ff12 = os.path.join(path, file12)
ff13 = os.path.join(path, file13)
ff14 = os.path.join(path, file14)
ff15 = os.path.join(path, file15)
ff16 = os.path.join(path, file16)
ff17 = os.path.join(path, file17)


# In[4]:


BPP3COLS = pd.read_excel(ff1, engine="openpyxl")
BPP3COLS = BPP3COLS.iloc[:, 0].tolist()


# In[5]:


dfdesc3COLS = pd.read_excel(ff2, engine="openpyxl")
dfdesc3COLS = dfdesc3COLS.iloc[:, 0].tolist()


# In[6]:


PPC3COLS = pd.read_excel(ff3, engine="openpyxl")
PPC3COLS = PPC3COLS.iloc[:, 0].tolist()


# In[7]:


PPM3COLS = pd.read_excel(ff4, engine="openpyxl")
PPM3COLS = PPM3COLS.iloc[:, 0].tolist()


# In[8]:


SPC3COLS = pd.read_excel(ff5, engine="openpyxl")
SPC3COLS = SPC3COLS.iloc[:, 0].tolist()


# In[9]:


SPM3COLS = pd.read_excel(ff6, engine="openpyxl")
SPM3COLS = SPM3COLS.iloc[:, 0].tolist()


# In[10]:


SUBPC3COLS = pd.read_excel(ff7, engine="openpyxl")
SUBPC3COLS = SUBPC3COLS.iloc[:, 0].tolist()


# In[11]:


SUBPM3COLS = pd.read_excel(ff8, engine="openpyxl")
SUBPM3COLS = SUBPM3COLS.iloc[:, 0].tolist()


# In[12]:


BPP4COLS = pd.read_excel(ff9, engine="openpyxl")
BPP4COLS = BPP4COLS.iloc[:, 0].tolist()


# In[13]:


dfdesc4COLS = pd.read_excel(ff10, engine="openpyxl")
dfdesc4COLS = dfdesc4COLS.iloc[:, 0].tolist()


# In[14]:


dfcorrlat4COLS = pd.read_excel(ff11, engine="openpyxl")
dfcorrlat4COLS = dfcorrlat4COLS.iloc[:, 0].tolist()


# In[15]:


PPC4COLS = pd.read_excel(ff12, engine="openpyxl")
PPC4COLS= PPC4COLS.iloc[:, 0].tolist()


# In[16]:


PPM4COLS = pd.read_excel(ff13, engine="openpyxl")
PPM4COLS = PPM4COLS.iloc[:, 0].tolist()


# In[17]:


SPC4COLS= pd.read_excel(ff14, engine="openpyxl")
SPC4COLS = SPC4COLS.iloc[:, 0].tolist()


# In[18]:


SPM4COLS= pd.read_excel(ff15, engine="openpyxl")
SPM4COLS = SPM4COLS.iloc[:, 0].tolist()


# In[19]:


SUBPC4COLS= pd.read_excel(ff16, engine="openpyxl")
SUBPC4COLS = SUBPC4COLS.iloc[:, 0].tolist()


# In[20]:


SUBPM4COLS= pd.read_excel(ff17, engine="openpyxl")
SUBPM4COLS = SUBPM4COLS.iloc[:, 0].tolist()


# In[ ]:






# In[ ]:




