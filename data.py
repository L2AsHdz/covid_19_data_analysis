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



# ------------------------------------ Limpieza y transformacion de csv global
# Renombrar columnas
global_df.rename(columns={'ï»¿Date_reported': 'date_reported'}, inplace=True)
global_df.rename(columns={'Country_code': 'country_code'}, inplace=True)
global_df.rename(columns={'Country': 'country'}, inplace=True)
global_df.rename(columns={'New_cases': 'new_cases'}, inplace=True)
global_df.rename(columns={'Cumulative_cases': 'cumulative_cases'}, inplace=True)
global_df.rename(columns={'New_deaths': 'new_deaths'}, inplace=True)
global_df.rename(columns={'Cumulative_deaths': 'cumulative_deaths'}, inplace=True)
global_df.rename(columns={'WHO_region': 'who_region'}, inplace=True)

# Filtrar datos para guatemala
global_df = global_df[global_df['country_code'] == 'GT'].copy()

# Eliminacion de columnas innecesarias
columns_to_delete = ['who_region', 'country_code', 'country']
global_df.drop(columns_to_delete, axis=1, inplace=True)

# Validar tipo de datos
global_df['date_reported'] = pd.to_datetime(global_df['date_reported'], errors='coerce', format='%m/%d/%Y')
global_df = global_df.dropna(subset=['date_reported'])
global_df = global_df[global_df['new_cases'].astype(str).str.isdigit()]
global_df = global_df[global_df['cumulative_cases'].astype(str).str.isdigit()]
global_df = global_df[global_df['new_deaths'].astype(str).str.isdigit()]
global_df = global_df[global_df['cumulative_deaths'].astype(str).str.isdigit()]

# Conversion de tipos
global_df['date_reported'] = pd.to_datetime(global_df['date_reported'], format='%m/%d/%Y')
global_df['new_cases'] = global_df['new_cases'].astype(int)
global_df['cumulative_cases'] = global_df['cumulative_cases'].astype(int)
global_df['new_deaths'] = global_df['new_deaths'].astype(int)
global_df['cumulative_deaths'] = global_df['cumulative_deaths'].astype(int)

# Filtrar por año
global_df = global_df[global_df['date_reported'].dt.year == config["filter_year"]].copy()

# Eliminar duplicados
global_df.drop_duplicates(subset=['date_reported'], inplace=True)

# Ordenar en base a fecha
global_df = global_df.sort_values(by='date_reported')

# Manejo de datos faltantes y estandarizacion de campos
global_df = global_df.dropna(how='all')

global_df['new_cases'] = global_df['new_cases'].fillna(0)
global_df['cumulative_cases'] = global_df['cumulative_cases'].ffill()
global_df['new_deaths'] = global_df['new_deaths'].fillna(0)
global_df['cumulative_deaths'] = global_df['cumulative_deaths'].ffill()


# -------------------------------------- Limpieza y transformacion de csv local
# Filtrar datos para 22 departamentos
local_df = local_df[(local_df['codigo_departamento'] >= 1) & (local_df['codigo_departamento'] <= 22)].copy()

# Validar tipo de datos
regex = r"^[a-zA-Z0-9\sáéíóúÁÉÍÓÚüÜñÑ]+$"
local_df = local_df[local_df['departamento'].str.match(regex, na=False)]
local_df = local_df[local_df['municipio'].str.match(regex, na=False)]
local_df = local_df[local_df['codigo_departamento'].astype(str).str.isdigit()]
local_df = local_df[local_df['codigo_municipio'].astype(str).str.isdigit()]
local_df = local_df[local_df['poblacion'].astype(str).str.isdigit()]

# Duplicados de departamentos
codigos_departamentos = local_df.groupby('departamento')['codigo_departamento'].agg(lambda x: x.value_counts()
                                                                                             .idxmax()).reset_index()
local_df = pd.merge(local_df, codigos_departamentos, on=['departamento', 'codigo_departamento'])


# Dar vuelta a las columnas de fecha
local_df = local_df.melt(id_vars=['codigo_departamento', 'departamento', 'codigo_municipio',
                                                    'municipio', 'poblacion'], var_name='fecha', value_name='muertes')

# Validar fechas
local_df['fecha'] = pd.to_datetime(local_df['fecha'], format='%m/%d/%Y')

# Filtrar por año
local_df = local_df[local_df['fecha'].dt.year == config["filter_year"]].copy()

# Eliminar duplicados
local_df.drop_duplicates(subset=['codigo_departamento', 'codigo_municipio', 'fecha'], inplace=True)

# Filtrar por registros con casos mayores a cero
local_df = local_df[local_df['muertes'] >= 0].copy()

# Ordenar en base a fecha
local_df = local_df.sort_values(by='fecha')

# Estandarizar datos string a mayusculas
local_df['departamento'] = local_df['departamento'].str.upper()
local_df['municipio'] = local_df['municipio'].str.upper()

print(local_df.dtypes)


# ------------------------------------- Obtener departamentos y municipios
# Obtener departamentos con codigo
departamentos = local_df[['codigo_departamento', 'departamento']].drop_duplicates().copy()
departamentos = departamentos.sort_values(by='codigo_departamento')

# Obtener municipios con codigo
municipios = local_df[['codigo_municipio', 'municipio', 'codigo_departamento', 'poblacion']].drop_duplicates().copy()
municipios = municipios.sort_values(by='codigo_municipio')

print(departamentos)
print(municipios)


# ------------------------------------------------- Unir dataframes
# Obtener registros con codigo_departamento == 1
capital = local_df[local_df['codigo_departamento'] == 1].copy()

# Obtener registros con codigo_departamento diferente de 1
interior = local_df[local_df['codigo_departamento'] != 1].copy()

# Eliminacion de columnas innecesarias
columns_to_delete = ['codigo_departamento', 'departamento', 'municipio', 'poblacion']
local_df.drop(columns_to_delete, axis=1, inplace=True)

# Conteo capital
merged_df = pd.merge(global_df, capital.groupby('fecha')['muertes'].sum().reset_index(), left_on='date_reported',
                     right_on='fecha', how='left')
merged_df = merged_df.rename(columns={'muertes': 'muertes_capital'})

# Conteo interior
merged_df = pd.merge(merged_df, interior.groupby('fecha')['muertes'].sum().reset_index(), left_on='date_reported',
                     right_on='fecha', how='left')
merged_df = merged_df.rename(columns={'muertes': 'muertes_interior'})


# Eliminacion de columnas innecesarias
columns_to_delete = ['fecha_x', 'fecha_y']
merged_df.drop(columns_to_delete, axis=1, inplace=True)
merged_df['date_reported'] = merged_df['date_reported'].dt.strftime('%Y-%m-%d')
local_df['fecha'] = local_df['fecha'].dt.strftime('%Y-%m-%d')
print(merged_df)