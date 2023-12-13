import pandas as pd
import numpy as np
import requests
import mariadb
import random
import time
import json
from io import StringIO

########################################################################################################################
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
########################################################################################################################


########################################################################################################################
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
########################################################################################################################


########################################################################################################################
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
########################################################################################################################


########################################################################################################################
# ------------------------------------- Obtener departamentos y municipios
# Obtener departamentos con codigo
departamentos = local_df[['codigo_departamento', 'departamento']].drop_duplicates().copy()
departamentos = departamentos.sort_values(by='codigo_departamento')

# Obtener municipios con codigo
municipios = local_df[['codigo_municipio', 'municipio', 'codigo_departamento', 'poblacion']].drop_duplicates().copy()
municipios = municipios.sort_values(by='codigo_municipio')

print(departamentos)
print(municipios)
########################################################################################################################


########################################################################################################################
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
########################################################################################################################


########################################################################################################################
# Variables para insercion y reportes
size = config["batch_size"]
no_bloques_insertados = 0
no_bloques_fallidos = 0

# almacenamiento de chunks con errores
chunks_departamentos = []
chunks_municipios = []
chunks_local_df = []
chunks_merged_df = []


def insert_chunk(chunk):
    global query
    global connection
    cursor = connection.cursor()

    try:
        data_to_insert = [tuple(row) for row in chunk.itertuples(index=False, name=None)]

        cursor.executemany(query, data_to_insert)
        connection.commit()

    except mariadb.Error as e:
        # Revertir la transacción en caso de error
        connection.rollback()
        print(f"Error en la transacción: {e}")
        raise e


def insert_dataframe_by_chunks(df, table_name):
    global no_bloques_insertados
    global no_bloques_fallidos

    global chunks_municipios
    global chunks_departamentos
    global chunks_local_df
    global chunks_merged_df

    print("----------------------------------------------")
    chunks = [df[i:i + size] for i in range(0, df.shape[0], size)]
    print(f"Insertando {df.shape[0]} registros en tabla {table_name}")
    for i, chunk in enumerate(chunks):
        try:
            print(f"Insertando lote {i + 1}: {chunk.shape[0]} registros")

            if config["simulate_errors"]:
                random_integer = random.randint(0, 1)
                if random_integer == 0:
                    raise mariadb.Error("Error en la transacción")
                else:
                    insert_chunk(chunk)
                    no_bloques_insertados += 1
            else:
                insert_chunk(chunk)
                no_bloques_insertados += 1

        except mariadb.Error as e:
            no_bloques_fallidos += 1

            if table_name == "departamento":
                chunks_departamentos.append(chunk)
            elif table_name == "municipio":
                chunks_municipios.append(chunk)
            elif table_name == "muertes_municipio":
                chunks_local_df.append(chunk)
            elif table_name == "general_data_by_fecha":
                chunks_merged_df.append(chunk)

# conexion a la base de datos
try:
    connection = mariadb.connect(
        user=config["db_user"],
        password=config["db_password"],
        host=config["db_host"],
        port=config["db_port"],
        database=config["db_name"]
    )

    # Insercion en tabla departamento

    query = "INSERT INTO departamento (codigo_departamento, nombre_departamento) VALUES (%s, %s)"
    insert_dataframe_by_chunks(departamentos, "departamento")

    # Insercion en tabla municipio
    query = "INSERT INTO municipio (codigo_municipio, nombre_municipio, codigo_departamento, poblacion) VALUES (%s, %s, %s, %s)"
    insert_dataframe_by_chunks(municipios, "municipio")

    # Insercion en tabla muertes_municipio
    query = "INSERT INTO muertes_municipio (codigo_municipio, fecha, muertes) VALUES (%s, %s, %s)"
    insert_dataframe_by_chunks(local_df, "muertes_municipio")

    # Insercion en tabla general_data_by_fecha
    query = "INSERT INTO general_data_by_fecha (fecha, casos_nuevos, casos_acumulados, muertes_nuevas, muertes_acumuladas, muertes_capital, muertes_interior) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    insert_dataframe_by_chunks(merged_df, "general_data_by_fecha")

    connection.close()
    print("Carga finalizada con exito\n")
    print(f"No. de bloques insertados: {no_bloques_insertados}")
    print(f"No. de bloques fallidos: {no_bloques_fallidos}")

    print("Reintentando bloques fallidos")
    time.sleep(5)
    # Reintentar bloques fallidos

    if len(chunks_departamentos) > 0:
        query = "INSERT INTO departamento (codigo_departamento, nombre_departamento) VALUES (%s, %s)"
        insert_dataframe_by_chunks(pd.concat(chunks_departamentos), "departamento")

    if len(chunks_municipios) > 0:
        query = "INSERT INTO municipio (codigo_municipio, nombre_municipio, codigo_departamento, poblacion) VALUES (%s, %s, %s, %s)"
        insert_dataframe_by_chunks(pd.concat(chunks_municipios), "municipio")

    if len(chunks_local_df) > 0:
        query = "INSERT INTO muertes_municipio (codigo_municipio, fecha, muertes) VALUES (%s, %s, %s)"
        insert_dataframe_by_chunks(pd.concat(chunks_local_df), "muertes_municipio")

    if len(chunks_merged_df) > 0:
        query = "INSERT INTO general_data_by_fecha (fecha, casos_nuevos, casos_acumulados, muertes_nuevas, muertes_acumuladas, muertes_capital, muertes_interior) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        insert_dataframe_by_chunks(pd.concat(chunks_merged_df), "general_data_by_fecha")



except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")

########################################################################################################################
