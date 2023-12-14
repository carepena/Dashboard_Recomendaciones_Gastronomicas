from google.cloud import storage
import pyarrow.parquet as pq
import pandas as pd
from io import BytesIO
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Cliente de Cloud Storage
client = storage.Client()

# Nombre del bucket
bucket_google = "empresas-goog"
bucket_archivos_actualizados = "actualizacion_de_archivos"

def etl_google(event, context):

    #Extraemos el archivo de business de google actualizado para realizarle todos los ETLs necesarios
    
    # nombre de archivo
    parquet_google_bisness = "business_google_actualizado.parquet"

    # Descarga el archivo Parquet desde Storage
    bucket = client.get_bucket(bucket_archivos_actualizados)
    blob = bucket.blob("archivos_yelp_actualizados/" + parquet_google_bisness)
    contenido = blob.download_as_bytes()

    # Lee el contenido del archivo Parquet en un DataFrame
    df_google_copy = pd.read_parquet(BytesIO(contenido))

    #Empezamos a normalizar nuestros datos

    #Eliminamos columnas que no son necesarias
    df_google_copy.drop(columns=['business_status','opening_hours','price_level','reference'], inplace=True)

    #extraemos lalitud y longitud
    # Aplicar json_normalize para convertir los diccionarios en un DataFrame
    location_google = pd.json_normalize(df_google_copy['geometry'])

    # Ahora, coordinates_df tiene dos columnas: 'location.lat' y 'location.lng'
    # Puedes unir este nuevo DataFrame con el original
    df_google_copy = df_google_copy.join(location_google[['location.lat', 'location.lng']])

    # Si deseas eliminar la columna 'geometry' original, puedes hacerlo
    df_google_copy = df_google_copy.drop('geometry', axis=1)

    # Renombrar las columnas si deseas tener nombres específicos como 'latitud' y 'longitud'
    df_google_copy = df_google_copy.rename(columns={'location.lat': 'latitud', 'location.lng': 'longitud'})


    ##Aqui extaeremos el codigo postal

    # Aplicar json_normalize para convertir los diccionarios en un DataFrame
    code_google = pd.json_normalize(df_google_copy['plus_code'])

    # Ahora, coordinates_df tiene dos columnas: 'location.lat' y 'location.lng'
    # Puedes unir este nuevo DataFrame con el original
    df_google_copy = df_google_copy.join(code_google[['compound_code', 'global_code']])

    # Si deseas eliminar la columna 'geometry' original, puedes hacerlo
    df_google_copy = df_google_copy.drop('plus_code', axis=1)

    # Renombrar las columnas si deseas tener nombres específicos como 'latitud' y 'longitud'
    df_google_copy = df_google_copy.rename(columns={'global_code': 'postal_code'})

    ##Extraemo city y address

    # Crear columnas vacías 
    df_google_copy['city'] = ''
    df_google_copy['address'] = ''

    # Iterar sobre las filas
    for index, row in df_google_copy.iterrows():
        # Obtener valor de vicinity
        vicinity = row.get('vicinity', '')  # Usa get para manejar la ausencia de la columna de forma segura
        
        # Usar expresión regular para extraer ciudad y dirección
        # La ciudad es después de la última coma, y la dirección es antes de la última coma
        if pd.isnull(vicinity):
            ciudad = ''
            direccion = ''
        else:
            # Dividir por la última coma y quitar espacios en blanco
            parts = vicinity.rsplit(',', 1)
            direccion = parts[0].strip()
            ciudad = parts[1].strip() if len(parts) > 1 else ''
        
        # Asignar valores a las columnas
        df_google_copy.at[index, 'city'] = ciudad
        df_google_copy.at[index, 'address'] = direccion

    # Eliminar la columna 'vicinity' fuera del bucle, solo una vez
    if 'vicinity' in df_google_copy.columns:
        df_google_copy.drop(columns='vicinity', inplace=True)



    # --------------------- AHORA TRABAJAREMOS CON LAS CATEGORIAS --------------------------

    # *******************************************************************************
    #Traemos el archivo a actualizar "busimess_category_google.parquet" -este contiene el business id con su category_id-
    
    # nombre de archivo
    busimess_category_google = "busimess_category_google.parquet"

    # Descarga el archivo Parquet desde Storage
    bucket = client.get_bucket(bucket_google)
    blob = bucket.blob(busimess_category_google)
    contenido = blob.download_as_bytes()

    # Lee el contenido del archivo Parquet en un DataFrame
    df_google_categoria_business = pd.read_parquet(BytesIO(contenido))

    #**********************************************************************************

    # **********************************************************************************
    #Extraemos el archivo a actualizar "category_google.parquet" -este contiene las categorias con sus Ids-

    # nombre de archivo
    category_google = "category_google.parquet"

    # Descarga el archivo Parquet desde Storage
    bucket = client.get_bucket(bucket_google)
    blob = bucket.blob(category_google)
    contenido = blob.download_as_bytes()

    # Lee el contenido del archivo Parquet en un DataFrame
    df_google_categoria = pd.read_parquet(BytesIO(contenido))
    # **********************************************************************************



    # Convierte cada lista de la columna 'types' en un string separado por comas -cada palabra queda separada por comas unicamente-
    df_google_copy['types'] = df_google_copy['types'].apply(lambda x: ', '.join(x))

    df_google_copy_categoria= df_google_copy[['place_id','types']]

    ## Actualizamos el archivo "df_google_categoria" y agregamos el id de las categorias a nuestro df df_google_copy_categoria
    # Diccionario para mapear categorías a ids
    category_to_id = dict(zip(df_google_categoria['category'], df_google_categoria['category_id']))

    # Lista para nuevas categorías
    new_categories = [] 

    # Iterar filas de df_categorias
    for index, row in df_google_copy_categoria.iterrows():

        # Lista para ids 
        category_ids = []

        # Iterar categorías 
        for category in row['types'].split(', '):
            
            # Revisar si está en el diccionario
            if category in category_to_id:
                category_ids.append(category_to_id[category])

            # Si no, agregar nueva    
            else:
                new_id = len(category_to_id) + 1
                category_to_id[category] = new_id  
                new_categories.append(category)
                category_ids.append(new_id)

    # Convertir lista a string   
    category_ids = str(category_ids).replace('[','').replace(']','')

    # Asignar ids a la fila
    df_google_copy_categoria.at[index, 'category_id'] = category_ids

    # Agregar nuevas categorías 
    for category in new_categories:
        new_id = len(df_google_categoria) + 1
        df_google_categoria.loc[len(df_google_categoria)] = [new_id, category]

    #Eliminamos la columna type de nuestro dataframe porque unicamente lo identificaremos con el id
    df_google_copy_categoria.drop(columns=['types'],inplace=True)
    

    ## Ahora organizaremos de manera correcta los ids, ya que en una misma fila tenemos muchos numeros de los id(esto se debe a que un solo business tiene muchas categorias/types asignadas)
    # Lista para ir agregando filas
    rows = []

    for index, row in df_google_copy_categoria.iterrows():

        ids = row['category_id'].split(', ')
        
        for id in ids:
        
            # Agregar diccionario a la lista
            rows.append({'place_id': row['place_id'], 'category_id': id})

    # Convertir la lista a DataFrame
    new_df_google = pd.DataFrame(rows)

    # Resetear índice
    new_df_google = new_df_google.reset_index(drop=True)

    new_df_google

    ## Cambiamos el nombre de nuestro identificador(id) que es es plac_id
    new_df_google.rename(columns={'place_id':'gmap_id'}, inplace=True)

    ##Actualizamos el dataframe df_google_categoria_business comparando y actualizando  su contenido con nuestro df
    # Lista para nuevas filas 
    new_rows = []

    for index, row in new_df_google.iterrows():

        if row['gmap_id'] not in df_google_categoria_business['gmap_id'].values:
            
            new_rows.append(row)
            
        else:
            
            df_google_categoria_business.loc[df_google_categoria_business['gmap_id'] == row['gmap_id'], 'category_id'] = row['category_id']

    # Concatenar df_google_categoria_business con nuevas filas   
    df_google_categoria_business = pd.concat([df_google_categoria_business, pd.DataFrame(new_rows)], ignore_index=True) 

    df_google_categoria_business = df_google_categoria_business.reset_index(drop=True)

    #//////////////////////////////////////////////////////////////////////////
    #CARGAMOS DFs ACTUALIZADOS...

    # Guarda el DataFrame modificado de nuevo como un archivo Parquet en Storage


    bucket = client.get_bucket(bucket_google)
    nuevo_nombre_archivo = busimess_category_google
    nuevo_blob = bucket.blob(busimess_category_google)
    nuevo_blob.upload_from_string(df_google_categoria_business.to_parquet(), content_type='application/octet-stream')



    bucket = client.get_bucket(bucket_google)
    nuevo_nombre_archivo = category_google
    nuevo_blob = bucket.blob(category_google)
    nuevo_blob.upload_from_string(df_google_copy_categoria.to_parquet(), content_type='application/octet-stream')

    #/////////////////////////////////////////////////////////////////////////

    #-------------------------- REVIEWS ----------------------------------------

    ##Función del analisis de sentimiento

    # Creamos una instancia para el analizador de sentimientos de Vader
    sentiment_analyzer = SentimentIntensityAnalyzer()

    def categoriza_sentimiento(texto):
        # Función que clasifica los sentimientos en base al cálculo del tono emocional de los comentarios
        # y les asigna un valor de 0 (malo), 1 (neutral) o 2 (positivo)

        retorno = (1, 0)  # Inicia una variable con el valor para la categoría de Neutro
        if (not pd.isna(texto)) and (len(texto) > 0):
            sentimiento = sentiment_analyzer.polarity_scores(texto)["compound"]
            if sentimiento >= 0.33:
                retorno = (2, sentimiento)  # Categoría de Positivo
            elif sentimiento <= -0.33:
                retorno = (0, sentimiento)  # Categoría de Negativo

        return retorno

    #Extraemos el archivo de business de google actualizado para realizarle todos los ETLs necesarios
    
    # nombre de archivo
    parquet_google_reviews = "reviews_google_actualizado.parquet"

    # Descarga el archivo Parquet desde Storage
    bucket = client.get_bucket(bucket_archivos_actualizados)
    blob = bucket.blob("archivos_yelp_actualizados/" + parquet_google_reviews)
    contenido = blob.download_as_bytes()

    # Lee el contenido del archivo Parquet en un DataFrame
    df_reviews = pd.read_parquet(BytesIO(contenido))

    #Empezamos a normalizar nuestros datos

    ##Normalizamos time
    df_reviews['time'] = pd.to_datetime(df_reviews['time'], unit='s')

    ## Aplicamos el analisis de sentimiento
    df_reviews[["sentiment", "sentiment_score"]] = df_reviews["text"].apply(categoriza_sentimiento).apply(pd.Series)
    df_reviews["sentiment"] = df_reviews["sentiment"].astype(int)


    # ------------ CAMBIOS PARA LA ACTUALIZACION DE LAS TABLAS BUSINESS Y REVIEWS ----------------------

    # ------------ Business actualización -----------------

    #Esto lo utilizamos unicamente para poder agregarle la columna state a la tabla de reviews
    states = ['CA', 'FL', 'PA', 'IL', 'NJ']

    # Función para extraer el estado de la columna 'compound_code'
    def extract_state(compound_code):
        for state in states:
            if f", {state}," in compound_code:
                return state
        return None  # Si no se encuentra ninguna coincidencia, devuelve None

    # Aplica la función para crear una nueva columna 'state'
    df_google_copy['state'] = df_google_copy['compound_code'].apply(extract_state)


    # Utilizamos el merge para unir los DataFrames en la columna 'place_id'
    df_merged = pd.merge(df_reviews, df_google_copy[['place_id', 'state']], on='place_id', how='left')



    ##Eliminamos columnas que nos quedaron inecesarias en la tabla de business
    df_google_copy.drop(columns=['types', 'compound_code','state'], inplace=True)

    ## Reemplazamos los nombres de las columnas
    dic_cambio ={
    'place_id':'gmap_id',
    'city':'city_id',
    'postal_code':'postal_code_id',
    'latitud':'latitude',
    'longitud':'longitude',
    'rating':'avg_rating',
    'user_ratings_total':'num_of_reviews'
    }
    df_google_copy.rename(columns=dic_cambio, inplace=True)

    ##Agregamos la columna que no nos proporciona la API pero, que si teniamos en nuestro archivos bases
    df_google_copy['description']=None

    ## Reeordenamos las columnas
    df_google_copy = df_google_copy[['gmap_id', 'name', 'description', 'address', 'city_id', 'postal_code_id', 'latitude', 'longitude', 'avg_rating', 'num_of_reviews']]

    ##Extraemos el archivo bussines de google que tenemos originalmente para actualizarlo
    # nombre de archivo
    parquet_business_google = "business_google.parquet"

    # Descarga el archivo Parquet desde Storage
    bucket = client.get_bucket(bucket_google)
    blob = bucket.blob(parquet_business_google)
    contenido = blob.download_as_bytes()

    # Lee el contenido del archivo Parquet en un DataFrame
    df_business_google_referencia = pd.read_parquet(BytesIO(contenido))

    ##Actualizamos
    # Establecer 'gmap_id' como el índice para ambas tablas para facilitar la combinación
    df_google_copy.set_index('gmap_id', inplace=True)
    df_business_google_referencia.set_index('gmap_id', inplace=True)

    # Actualizar y agregar las filas de df_google_copy a df_business_google_referencia
    updated_business_google = df_google_copy.combine_first(df_business_google_referencia)

    # Restablecer el índice para volver a tener 'gmap_id' como una columna
    updated_business_google.reset_index(inplace=True)


    ## Cargamos los datos actualizados
    # Guardamos el DataFrame modificado de nuevo como un archivo Parquet en Storage
    bucket = client.get_bucket(bucket_google)
    nuevo_nombre_archivo = parquet_business_google
    nuevo_blob = bucket.blob(nuevo_nombre_archivo)
    nuevo_blob.upload_from_string(updated_business_google.to_parquet(), content_type='application/octet-stream')


    # -------Reviews actualizacion-----------
    
    ##Cambiamos los nombres de las columnas de nuestra tabla de reviews que ahora se llama df_merge
    dic_cambio_reviews={
    'place_id':'gmap_id',
    'author_name':'name'
    }
    df_merged.rename(columns=dic_cambio_reviews, inplace=True)

    #Reeordenamos las columnas
    df_merged=df_merged[['gmap_id', 'user_id', 'name', 'time', 'rating', 'text', 'state', 'sentiment', 'sentiment_score']]

    ##Extraemos el archivo reviews de google que tenemos originalmente para actualizarlo
    # nombre de archivo
    parquet_reviews_google = "reviews_google.parquet"

    # Descarga el archivo Parquet desde Storage
    bucket = client.get_bucket(bucket_google)
    blob = bucket.blob(parquet_reviews_google)
    contenido = blob.download_as_bytes()

    # Lee el contenido del archivo Parquet en un DataFrame
    df_reviews_google_referencia = pd.read_parquet(BytesIO(contenido))

    #Actualizamos
    # Establecer 'gmap_id' como el índice para ambas tablas para facilitar la combinación
    df_merged.set_index('gmap_id', inplace=True)
    df_reviews_google_referencia.set_index('gmap_id', inplace=True)

    # Actualizar y agregar las filas de df_merged a df_reviews_google_referencia
    updated_df_reviews = df_merged.combine_first(df_reviews_google_referencia)

    # Restablecer el índice para volver a tener 'gmap_id' como una columna
    updated_df_reviews.reset_index(inplace=True)

    
    ## Cargamos los datos actualizados
    # Guardamos el DataFrame modificado de nuevo como un archivo Parquet en Storage
    bucket = client.get_bucket(bucket_google)
    nuevo_nombre_archivo = parquet_reviews_google
    nuevo_blob = bucket.blob(nuevo_nombre_archivo)
    nuevo_blob.upload_from_string(updated_df_reviews.to_parquet(), content_type='application/octet-stream')



    print('se realizó el ETL de manera correcta')