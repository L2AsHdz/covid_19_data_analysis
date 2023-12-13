# Análisis y Carga de Datos sobre COVID-19 en Guatemala

## Tecnologías Utilizadas
- **Python:** Lenguaje de programación principal.
- **Pandas:** Biblioteca para manipulación y análisis de datos.
- **NumPy:** Biblioteca para operaciones numéricas en Python.
- **Requests:** Biblioteca para realizar solicitudes HTTP.
- **MariaDB:** Sistema de gestión de bases de datos utilizado.
- **JSON:** Formato de archivo para almacenar configuraciones.
- **io.StringIO:** Para la lectura de datos CSV desde una cadena.

## Archivo de Configuración

### Atributos del Archivo JSON:
El archivo `config.json` contiene los siguientes atributos:

- **local_csv_name:** Nombre del archivo CSV local que contiene datos sobre municipios.
  
- **url_global_csv:** URL que apunta al archivo CSV global que contiene datos sobre la COVID-19 a nivel mundial.

- **filter_year:** Año utilizado como filtro para seleccionar datos relevantes en el análisis.

- **batch_size:** Tamaño del lote utilizado para la inserción eficiente de datos en la base de datos.

- **db_user:** Usuario de la base de datos MariaDB.

- **db_password:** Contraseña del usuario de la base de datos.

- **db_host:** Dirección del host de la base de datos.

- **db_port:** Puerto de conexión a la base de datos.

- **db_name:** Nombre de la base de datos utilizada para almacenar los datos procesados.

## Descripción del Proceso ETL

El script realiza la carga, limpieza, transformación y almacenamiento de datos relacionados con la COVID-19 en Guatemala. A continuación, se detallan las principales secciones del código:

### 1. Cargar Configuración
- Se lee el archivo de configuración `config.json` que contiene detalles como nombres de archivos, URLs, año de filtro y configuración de la base de datos.

### 2. Cargar Dataframes
- Se cargan dos DataFrames de pandas: uno desde un archivo CSV local (`local_df`) y otro desde una URL que apunta a un archivo CSV global (`global_df`).

### 3. Limpieza y Transformación de CSV Global
- Corrección de nombres de columnas.
- Filtrado para datos específicos de Guatemala.
- Eliminación de columnas innecesarias.
- Validación y conversión de tipos de datos.
- Filtrado por año.
- Eliminación de duplicados.
- Ordenamiento por fecha.
- Manejo de datos faltantes y estandarización de campos.

### 4. Limpieza y Transformación de CSV Local
- Validación de tipos de datos.
- Eliminación de duplicados.
- Filtrado por año.
- Eliminación de registros con muertes negativas.

### 5. Obtener Departamentos y Municipios
- Extracción de DataFrames con información única sobre departamentos y municipios.

### 6. Unir Dataframes
- Creación de DataFrames adicionales filtrando por código de departamento.
- Unión con `global_df` y generación de columnas adicionales.

### 7. Inserción en Base de Datos
- Inserción eficiente por lotes en tablas de la base de datos utilizando `mariadb`.

### 8. Reporte de Resultados
- Impresión de información sobre la carga, incluyendo el número de bloques insertados y fallidos.

Este script automatiza el proceso de preparación de datos para análisis posterior sobre la evolución de la COVID-19 en Guatemala, facilitando la carga y transformación de datos desde fuentes locales y globales en un entorno de base de datos relacional.
