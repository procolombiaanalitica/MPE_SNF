#!/usr/bin/env python
# coding: utf-8

# LIBERIAS Y PAQUETES

# In[ ]:


import pandas as pd
import numpy as np
import time
import re
import warnings
warnings.filterwarnings('ignore')
from helpers import get_session_info, update_session_params, clean_column_name
import fsspec
from io import StringIO
import zipfile
import os
from pathlib import Path
import RenameCols as mapa


# In[2]:


# Aumentar número de columnas que se pueden ver
pd.options.display.max_columns = None
# En los dataframes, mostrar los float con dos decimales
pd.options.display.float_format = '{:,.10f}'.format
# Cada columna será tan grande como sea necesario para mostrar todo su contenido
pd.set_option('display.max_colwidth', 0)


# In[3]:


# Librerias necesarias para subir a Snowflake
import os
import json
import snowflake.connector 
#pip install snowflake-connector-python
from snowflake.connector.pandas_tools import write_pandas 
#pip install "snowflake-connector-python[pandas]"
from snowflake.snowpark import Session


# SESIÓN

# In[4]:


# Paso 1: Definir la ruta al archivo JSON en el escritorio
desktop_path = "c:\\Users\\PGIC2\\OneDrive - PROCOLOMBIA\\Escritorio\\CREDENCIALES"
json_file_path = os.path.join(desktop_path, "CREDENCIALANALITICA.json")
json_file_path
 


# In[5]:


# Paso 2: Leer las credenciales desde el archivo JSON
with open(json_file_path, 'r') as file:
    credentials = json.load(file)
 
# Paso 3: Definir los parámetros de conexión usando las credenciales
connection_parameters = {
        "account": credentials["ACCOUNT_SNOWFLAKE"],
        "user": credentials["USER_SNOWFLAKE"],
        "password": credentials["PASSWORD_SNOWFLAKE"],
        "role": credentials["ROLE_SNOWFLAKE"],
        "warehouse": credentials["WAREHOUSE"]
    }
 
# Paso 5: Crear un objeto de conexión utilizando snowflake.connector
session = Session.builder.configs(connection_parameters).create()
print("Sesión actual:", {session})


# In[6]:


#Crear objeto de conexión llamado conn
conn=session.connection


# In[7]:


# Crear un cursor para ejecutar consultas (No olvidar cerrar el curso y la conexión al terminar el proces)
cur = conn.cursor()
cur


# In[8]:


# Asegurar que estamos en la ubicación que se desea para subir las bases de datos
cur.execute("SELECT CURRENT_WAREHOUSE() AS WAREHOUSE, CURRENT_DATABASE() AS DATABASE, CURRENT_SCHEMA() AS SCHEMA;")
cur.fetchone()


# In[9]:


cur.execute("USE DATABASE CARGUE_ARCHIVOS_MPE_BD")


# In[10]:


cur.execute("USE SCHEMA PRODUCTOS_POTENCIALES")


# In[11]:


# Asegurar que estamos en la ubicación que se desea para subir las bases de datos
cur.execute("SELECT CURRENT_WAREHOUSE() AS WAREHOUSE, CURRENT_DATABASE() AS DATABASE, CURRENT_SCHEMA() AS SCHEMA;")
cur.fetchone()


# RENAME COLS PARA UNIFICAR FORMATO CON ORIGEN

# PRODUCTOS POTENCIALES

# In[12]:


query1 = """
select * from CARGUE_ARCHIVOS_MPE_BD.PRODUCTOS_POTENCIALES.TABLA_PROD_POTENCIALES          
"""


# In[13]:


BPP3OR = pd.DataFrame(session.sql(query1).collect())
BPP3OR.columns = mapa.BPP3COLS
base2=BPP3OR #en mpe.py se llama base2
base2


# In[14]:


query2 = """ 
select * from CARGUE_ARCHIVOS_MPE_BD.PRODUCTOS_POTENCIALES.DESC_PROD_POTENCIALES
"""


# In[15]:


dfdesc3OR = pd.DataFrame(session.sql(query2).collect())
dfdesc3OR.columns = mapa.dfdesc3COLS
dfdesc3OR = dfdesc3OR.drop(columns=['Unnamed: 0'])
desc2=dfdesc3OR #en mpe.py se llama desc2
desc2


# In[16]:


query3 = """ 
select * from CARGUE_ARCHIVOS_MPE_BD.PRODUCTOS_POTENCIALES.PPC3
"""


# In[17]:


PPC3OR = pd.DataFrame(session.sql(query3).collect())
PPC3OR.columns = mapa.PPC3COLS
prod_pc2=PPC3OR #en mpe.py se llama prod_pc2
prod_pc2


# In[18]:


query4 = """ 
select * from CARGUE_ARCHIVOS_MPE_BD.PRODUCTOS_POTENCIALES.PPM3
"""


# In[19]:


PPM3OR = pd.DataFrame(session.sql(query4).collect())
PPM3OR.columns = mapa.PPM3COLS
prod_pm2=PPM3OR #en mpe.py se llama prod_pm2
prod_pm2


# In[20]:


query5 = """ 
select * from CARGUE_ARCHIVOS_MPE_BD.PRODUCTOS_POTENCIALES.SPC3
"""


# In[21]:


SPC3OR = pd.DataFrame(session.sql(query5).collect())
SPC3OR.columns = mapa.SPC3COLS
sec_pc2=SPC3OR #en mpe.py se llama sec_pc2
sec_pc2


# In[22]:


query6 = """ 
select * from CARGUE_ARCHIVOS_MPE_BD.PRODUCTOS_POTENCIALES.SPM3
"""


# In[23]:


SPM3OR = pd.DataFrame(session.sql(query6).collect())
SPM3OR.columns = mapa.SPM3COLS
sec_pm2=SPM3OR #en mpe.py se llama sec_pm2
sec_pm2


# In[24]:


query7 = """ 
select * from CARGUE_ARCHIVOS_MPE_BD.PRODUCTOS_POTENCIALES.SUBPC3
"""


# In[25]:


SUBPC3OR = pd.DataFrame(session.sql(query7).collect())
SUBPC3OR.columns = mapa.SUBPC3COLS
sub_pc2=SUBPC3OR #en mpe.py se llama sub_pc2
sub_pc2


# In[26]:


query8 = """ 
select * from CARGUE_ARCHIVOS_MPE_BD.PRODUCTOS_POTENCIALES.SUBPM3
"""


# In[27]:


SUBPM3OR = pd.DataFrame(session.sql(query8).collect())
SUBPM3OR.columns = mapa.SUBPM3COLS
sub_pm2=SUBPM3OR #en mpe.py se llama sub_pm2
sub_pm2


# PAISES POTENCIALES

# In[28]:


cur.execute("USE SCHEMA PAISES_POTENCIALES")


# In[29]:


# Asegurar que estamos en la ubicación que se desea para subir las bases de datos
cur.execute("SELECT CURRENT_WAREHOUSE() AS WAREHOUSE, CURRENT_DATABASE() AS DATABASE, CURRENT_SCHEMA() AS SCHEMA;")
cur.fetchone()


# In[30]:


query9 = """
select * from CARGUE_ARCHIVOS_MPE_BD.PAISES_POTENCIALES.TABLA_PAISES_POTENCIALES
"""


# In[31]:


BPP4OR=pd.DataFrame(session.sql(query9).collect())
BPP4OR.columns = mapa.BPP4COLS
base=BPP4OR #en mpe.py se llama base
base


# In[32]:


query10 = """
select * from CARGUE_ARCHIVOS_MPE_BD.PAISES_POTENCIALES.CORR_PAISES_POTENCIALES
"""


# In[33]:


dfcorrlat4OR=pd.DataFrame(session.sql(query10).collect())
dfcorrlat4OR.columns = mapa.dfcorrlat4COLS
corr=dfcorrlat4OR #en mpe.py se llama corr
corr


# In[34]:


query11 = """
select * from CARGUE_ARCHIVOS_MPE_BD.PAISES_POTENCIALES.DESC_PAISES_POTENCIALES
"""


# In[35]:


dfdesc4OR=pd.DataFrame(session.sql(query11).collect())
dfdesc4OR.columns = mapa.dfdesc4COLS
dfdesc4OR = dfdesc4OR.drop(columns=['Unnamed: 0'])
desc=dfdesc4OR #en mpe.py se llama desc
desc


# In[36]:


query12 = """
select * from CARGUE_ARCHIVOS_MPE_BD.PAISES_POTENCIALES.PPC4
"""


# In[37]:


PPC4OR=pd.DataFrame(session.sql(query12).collect())
PPC4OR.columns = mapa.PPC4COLS
prod_pc=PPC4OR #en mpe.py se llama prod_pm
prod_pc


# In[38]:


query13 = """
select * from CARGUE_ARCHIVOS_MPE_BD.PAISES_POTENCIALES.PPM4
"""


# In[39]:


PPM4OR=pd.DataFrame(session.sql(query13).collect())
PPM4OR.columns = mapa.PPM4COLS

prod_pm=PPM4OR #en mpe.py se llama prod_pm
prod_pm


# In[40]:


query14 = """
select * from CARGUE_ARCHIVOS_MPE_BD.PAISES_POTENCIALES.SPC4
"""


# In[41]:


SPC4OR=pd.DataFrame(session.sql(query14).collect())
SPC4OR.columns = mapa.SPC4COLS
sec_pc=SPC4OR #en mpe.py se llama sec_pc
sec_pc


# In[42]:


query15 = """
select * from CARGUE_ARCHIVOS_MPE_BD.PAISES_POTENCIALES.SPM4
"""


# In[43]:


SPM4OR=pd.DataFrame(session.sql(query15).collect())
SPM4OR.columns = mapa.SPM4COLS
sec_pm=SPM4OR #en mpe.py se llama sec_pm
sec_pm


# In[44]:


query16 = """
select * from CARGUE_ARCHIVOS_MPE_BD.PAISES_POTENCIALES.SUBPC4
"""


# In[45]:


SUBPC4OR=pd.DataFrame(session.sql(query16).collect())
SUBPC4OR.columns = mapa.SUBPC4COLS
sub_pc=SUBPC4OR #en mpe.py se llama sub_pc
sub_pc


# In[46]:


query17 = """
select * from CARGUE_ARCHIVOS_MPE_BD.PAISES_POTENCIALES.SUBPM4
"""


# In[47]:


SUBPM4OR=pd.DataFrame(session.sql(query17).collect())
SUBPM4OR.columns = mapa.SUBPM4COLS
sub_pm=SUBPM4OR #en mpe.py se llama sub_pm
sub_pm


# In[48]:


#cerrar las conexiones para ahorrar creditos
#conn.close()
#cur.close()
#session.close()

