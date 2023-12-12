import pandas as pd
import numpy as np
import requests
import json
from io import StringIO
from datetime import date

# Leer archivo de configuracion
with open('config.json') as f:
    config = json.load(f)

# Carga de dataframes

null_values = ['', 'n/a', 'NA', 'N/a', 'na', 'n/A', ' ', np.nan]

# Cargar csv local
local_df = pd.read_csv(config["local_csv_name"], na_values=null_values)

# Cargar csv global
url = config["url_global_csv"]
response = requests.get(url)
global_df = pd.read_csv(StringIO(response.text), na_values=null_values)



# Limpieza y transformacion de csv global

# Renombrar columnas
global_df.rename(columns={'ï»¿Date_reported': 'date_reported'}, inplace=True)
global_df.rename(columns={'Country_code': 'country_code'}, inplace=True)
global_df.rename(columns={'Country': 'country'}, inplace=True)
global_df.rename(columns={'New_cases': 'new_cases'}, inplace=True)
global_df.rename(columns={'Cumulative_cases': 'cumulative_cases'}, inplace=True)
global_df.rename(columns={'New_deaths': 'new_deaths'}, inplace=True)
global_df.rename(columns={'Cumulative_deaths': 'cumulative_deaths'}, inplace=True)
global_df.rename(columns={'WHO_region': 'who_region'}, inplace=True)

# Conversion de tipos
global_df['date_reported'] = pd.to_datetime(global_df['date_reported'], format='%m/%d/%Y')
global_df['new_cases'] = global_df['new_cases'].astype(int)
global_df['cumulative_cases'] = global_df['cumulative_cases'].astype(int)
global_df['new_deaths'] = global_df['new_deaths'].astype(int)
global_df['cumulative_deaths'] = global_df['cumulative_deaths'].astype(int)

# Filtrar datos para guatemala
filtered_df = global_df[global_df['country_code'] == 'GT'].copy()
filtered_df = filtered_df[filtered_df['date_reported'].dt.year == config["filter_year"]].copy()

# Eliminacion de columnas innecesarias
columns_to_delete = ['who_region', 'country_code', 'country']
filtered_df.drop(columns_to_delete, axis=1, inplace=True)

# Eliminar duplicados
filtered_df.drop_duplicates(subset=['date_reported'], inplace=True)

# Ordenar en base a fecha
df_ordenado = filtered_df.sort_values(by='date_reported')

# Manejo de datos faltantes y estandarizacion de campos
non_null_df = df_ordenado.dropna(how='all')

non_null_df['new_cases'] = non_null_df['new_cases'].fillna(0)
non_null_df['cumulative_cases'] = non_null_df['cumulative_cases'].fillna(method='ffill')
non_null_df['new_deaths'] = non_null_df['new_deaths'].fillna(0)
non_null_df['cumulative_deaths'] = non_null_df['cumulative_deaths'].fillna(method='ffill')

