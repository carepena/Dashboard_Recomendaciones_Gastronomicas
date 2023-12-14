from google.cloud import storage
import pyarrow.parquet as pq
import pandas as pd
from io import BytesIO
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Cliente de Cloud Storage
client = storage.Client()

# Nombre del bucket
bucket_yelp = "empresas-yelp"
bucket_archivos_actualizados = "actualizacion_de_archivos"

def etl_yelp(event, context):


    #Extraemos el archivo de business de yelp actualizado para realizarle todos los ETLs necesarios
    
    # nombre de archivo
    parquet_file = "business_yelp_actualizado.parquet"

    # Descarga el archivo Parquet desde Storage
    bucket = client.get_bucket(bucket_archivos_actualizados)
    blob = bucket.blob("archivos_yelp_actualizados/" + parquet_file)
    contenido = blob.download_as_bytes()

    # Lee el contenido del archivo Parquet en un DataFrame
    df_copia_yelp = pd.read_parquet(BytesIO(contenido))


    #Empezamos a normalizar nuestros datos


    # Aplicar json_normalize para convertir los diccionarios en un DataFrame
    coordinates_df = pd.json_normalize(df_copia_yelp['coordinates'])

    # Ahora, coordinates_df tiene dos columnas: 'latitude' y 'longitude'
    # Puedes unir este nuevo DataFrame con el original
    df_copia_yelp = df_copia_yelp.join(coordinates_df)

    # Si deseas eliminar la columna 'coordinates' original, puedes hacerlo
    df_copia_yelp = df_copia_yelp.drop('coordinates', axis=1)

    # Aplicar json_normalize para convertir los diccionarios en un DataFrame
    coordinates_df = pd.json_normalize(df_copia_yelp['location'])

    # Ahora, coordinates_df tiene dos columnas: 'latitude' y 'longitude'
    # Puedes unir este nuevo DataFrame con el original
    df_copia_yelp = df_copia_yelp.join(coordinates_df)

    # Si deseas eliminar la columna 'coordinates' original, puedes hacerlo
    df_copia_yelp = df_copia_yelp.drop('location', axis=1)

    #Eliminamos las columnas innecesarias

    df_copia_yelp.drop(columns=['display_address', 'address2', 'address3', 'country', 'image_url', 'alias', 'is_closed', 'url', 'price', 'phone', 'display_phone','distance','transactions'], inplace=True)

    # Lista para almacenar los nuevos valores de 'title'
    new_titles = []

    # Iterar sobre la columna 'categories'
    for categories_list in df_copia_yelp['categories']:
        # Inicializar lista temporal para 'title'
        title_list = []
        
        # Iterar sobre los diccionarios en la lista 'categories'
        for category_dict in categories_list:
            # Añadir 'title' a la lista temporal
            title_list.append(category_dict['title'])
        
        # Agregar la lista de 'title' a la lista final
        new_titles.append(title_list)

        #eliminamos la columna
    df_copia_yelp.drop(columns='categories', inplace=True)


    # Crear una nueva columna 'new_titles' en el DataFrame
    df_copia_yelp['categories'] = new_titles


    # Convertir la lista en cadena de texto con palabras separadas por comas
    df_copia_yelp['categories'] = df_copia_yelp['categories'].apply(lambda x: ', '.join(x).replace("[", "").replace("]", ""))


    ##Guardamos nuestro archivo Yelp Bussines al final...

    # ------------ AHORA VAMOS A CONFIGURAR Y EXTRAER LOS DATOS DE CATEGORIAS -------------------

    #Tenemos que extraer los datos que ya tenemos para compararlo y alli asignarle el id que le corresponde segun la categoria
    df_categoria=df_copia_yelp[['id','categories']]

    #*****************************************************************
    #Extraemos los datos que ya tenemos
    #Extraemos el archivo de category_yelp para compararlo y actualizarlo de acuerdo a los nuevos datos que tenemos

    # nombre de archivo
    parquet_file_empresas_category = "category_yelp.parquet"

    # Descarga el archivo Parquet desde Storage
    bucket = client.get_bucket(bucket_yelp)
    blob = bucket.blob("empresas-yelp/" + parquet_file_empresas_category)
    contenido = blob.download_as_bytes()

    # Lee el contenido del archivo Parquet en un DataFrame
    df_categoria_referencia = pd.read_parquet(BytesIO(contenido))


    # A partir de aqui podemos modificar nuestros datos

    # ******************************************************************

    # Diccionario para mapear categorías a ids
    category_to_id = dict(zip(df_categoria_referencia['category'], df_categoria_referencia['category_id']))

    # Lista para nuevas categorías
    new_categories = [] 

    # Iterar filas de df_categorias
    for index, row in df_categoria.iterrows():

        # Lista para ids 
        category_ids = []

        # Iterar categorías 
        for category in row['categories'].split(', '):
            
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
    df_categoria.at[index, 'category_id'] = category_ids

    # Agregar nuevas categorías 
    for category in new_categories:
        new_id = len(df_categoria_referencia) + 1
        df_categoria_referencia.loc[len(df_categoria_referencia)] = [new_id, category]


   
    # ******************************************************************************************************
 
    #Ahora necesitamos extraer empresas_yelp_business_category_yelp.parquet para campararlo y actualizar business category    
    
    # nombre de archivo
    parquet_file_business_categoria = "business_category_yelp.parquet"

    # Descarga el archivo Parquet desde Storage
    bucket = client.get_bucket(bucket_yelp)
    blob = bucket.blob("empresas-yelp/" + parquet_file_business_categoria)
    contenido = blob.download_as_bytes()

    # Lee el contenido del archivo Parquet en un DataFrame
    df_business_categoria = pd.read_parquet(BytesIO(contenido))


    # A partir de aqui podemos modificar nuestros datos

    # ******************************************************************************************************

    #eliminamos la coluimna de categoria
    df_categoria.drop(columns=['categories'], inplace=True)

    #cambiamos el nombre de la columna id
    df_categoria.rename(columns={'id':'business_id'}, inplace=True)

    # Lista para ir agregando filas
    rows = []

    for index, row in df_categoria.iterrows():

        ids = row['category_id'].split(', ')
        
        for id in ids:
        
            # Agregar diccionario a la lista
            rows.append({'business_id': row['business_id'], 'category_id': id})

    # Convertir la lista a DataFrame
    new_df = pd.DataFrame(rows)

    # Resetear índice
    new_df = new_df.reset_index(drop=True)


    # Lista para nuevas filas 
    new_rows = []

    for index, row in new_df.iterrows():

        if row['business_id'] not in df_business_categoria['business_id'].values:
            
            new_rows.append(row)
            
        else:
            
            df_business_categoria.loc[df_business_categoria['business_id'] == row['business_id'], 'category_id'] = row['category_id']

    # Concatenar df_business_categoria con nuevas filas   
    df_business_categoria = pd.concat([df_business_categoria, pd.DataFrame(new_rows)], ignore_index=True) 

    df_business_categoria = df_business_categoria.reset_index(drop=True)

    

    # 'df_business_categoria' es la tabla es la actualizacion de empresas_yelp_categoria.parquet
    
    #los datos entregados por henry que son entregados por yelp(academy) viene una columna de la cual la extraemos y creamos una tabla 'Atributtes'. Este dato unicamente viene en los datasets academicos, NO en la API.

    #////////////////////////////////////////////////////////////////////////

    # Guardamos el DataFrame de categorias con id de las empresas actualizado de nuevo como un archivo Parquet en Storage
    bucket = client.get_bucket(bucket_yelp)
    nuevo_nombre_archivo = parquet_file_empresas_category
    nuevo_blob = bucket.blob("empresas_yelp/" + nuevo_nombre_archivo)
    nuevo_blob.upload_from_string(df_categoria_referencia.to_parquet(), content_type='application/octet-stream')


    
    # Guardamos el DataFrame empreseas con id de categoria actualizado de nuevo como un archivo Parquet en Storage
    bucket = client.get_bucket(bucket_yelp)
    nuevo_nombre_archivo = parquet_file_business_categoria
    nuevo_blob = bucket.blob("empresas_yelp/" + nuevo_nombre_archivo)
    nuevo_blob.upload_from_string(df_business_categoria.to_parquet(), content_type='application/octet-stream')

    #////////////////////////////////////////////////////////////////////////

    #REVIEWS

    #Extraemos el archivo de reviews de yelp actualizado para realizarle todos los ETLs necesarios
    
    # nombre de archivo
    parquet_file_reviews = "reviews_yelp_actualizado.parquet"

    # Descarga el archivo Parquet desde Storage
    bucket = client.get_bucket(bucket_archivos_actualizados)
    blob = bucket.blob("archivos_yelp_actualizados/" + parquet_file_reviews)
    contenido = blob.download_as_bytes()

    # Lee el contenido del archivo Parquet en un DataFrame
    df_reviews_yelp_copy = pd.read_parquet(BytesIO(contenido))


    #Empezamos a normalizar nuestros datos


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

    # Eliminamos el nombre del autor ya que en Yelp no la estuvimos utilizando
    df_reviews_yelp_copy.drop(columns=['author_name'], inplace=True)

    #Aplicamos el analisis de sentimiento
    df_reviews_yelp_copy[["sentiment", "sentiment_score"]] = df_reviews_yelp_copy["text"].apply(categoriza_sentimiento).apply(pd.Series)

    df_reviews_yelp_copy["sentiment"] = df_reviews_yelp_copy["sentiment"].astype(int)

    
    # ------------ CAMBIOS PARA LA ACTUALIZACION DE LAS TABLAS BUSINESS Y REVIEWS ----------------------

    # ------------ Business actualización -----------------

    ##Reemplazamos los nombres de las columnas
    dic_cambio_yelp={
    'id':'business_id',
    'address1':'address',				    
    'city':'city_id',				
    'zip_code':'postal_code_id',		
    'rating':'stars'
    }
    df_copia_yelp.rename(columns=dic_cambio_yelp, inplace=True)

    ##Eliminamos las columnas inecesarias
    df_copia_yelp.drop(columns=['categories'], inplace=True)

    ##Creamos columna faltante
    df_copia_yelp['hours']=None

    ##Reordenamos las columnas
    df_copia_yelp=df_copia_yelp[['business_id','name','address','city_id','postal_code_id','latitude','longitude','stars','review_count','hours']]

    ##Extraemos el archivo bussines de yelp que tenemos originalmente para actualizarlo
    # nombre de archivo
    parquet_business_yelp = "business_yelp.parquet"

    # Descarga el archivo Parquet desde Storage
    bucket_yelp = "empresas-yelp"
    bucket = client.get_bucket(bucket_yelp)
    blob = bucket.blob("empresas_yelp/"+parquet_business_yelp)
    contenido = blob.download_as_bytes()

    # Lee el contenido del archivo Parquet en un DataFrame
    df_yelp_business_referencia = pd.read_parquet(BytesIO(contenido))

    #Actualizamos
    # Establecer 'business_id' como el índice para ambas tablas para facilitar la combinación
    df_copia_yelp.set_index('business_id', inplace=True)
    df_yelp_business_referencia.set_index('business_id', inplace=True)

    # Actualizar y agregar las filas de df_copia_yelp a df_yelp_business_referencia
    updated_df_yelp_business = df_copia_yelp.combine_first(df_yelp_business_referencia)

    # Restablecer el índice para volver a tener 'business_id' como una columna
    updated_df_yelp_business.reset_index(inplace=True)

    ## Cargamos los datos actualizados
    # Guardamos el DataFrame modificado de nuevo como un archivo Parquet en Storage
    bucket = client.get_bucket(bucket_yelp)
    nuevo_nombre_archivo = parquet_business_yelp
    nuevo_blob = bucket.blob(nuevo_nombre_archivo)
    nuevo_blob.upload_from_string(updated_df_yelp_business.to_parquet(), content_type='application/octet-stream')

    # -------Reviews actualizacion-----------
    
    ##Reenombramos los nombres de las columnas que necesitemos
    dic_cambio_yelp_reviews={'time_created':'date'}
    df_reviews_yelp_copy.rename(columns=dic_cambio_yelp_reviews, inplace=True)

    ##Eliminamos columnas inecesarias
    df_reviews_yelp_copy.drop(columns=['rating'], inplace=True)

    ##Creamos columnas que nos faltan
    df_reviews_yelp_copy['compliment_count']=0

    ##Reordenamos las columnas
    df_reviews_yelp_copy=df_reviews_yelp_copy[['user_id','business_id','text','date','compliment_count','sentiment','sentiment_score']]

    ##Extraemos el archivo reviews de google que tenemos originalmente para actualizarlo
    # nombre de archivo
    parquet_reviews_yelp = "reviews_yelp.parquet"

    # Descarga el archivo Parquet desde Storage
    bucket_yelp = "empresas-yelp"
    bucket = client.get_bucket(bucket_yelp)
    blob = bucket.blob("empresas_yelp/"+parquet_reviews_yelp)
    contenido = blob.download_as_bytes()

    # Lee el contenido del archivo Parquet en un DataFrame
    df_reviews_yelp_referencia = pd.read_parquet(BytesIO(contenido))

    #Actualizamos
    # Establecer 'business_id' como el índice para ambas tablas para facilitar la combinación
    df_reviews_yelp_copy.set_index('business_id', inplace=True)
    df_reviews_yelp_referencia.set_index('business_id', inplace=True)

    # Actualizar y agregar las filas de df_reviews_yelp_copy a df_reviews_yelp_referencia
    updated_df_reviews_yelp = df_reviews_yelp_copy.combine_first(df_reviews_yelp_referencia)

    # Restablecer el índice para volver a tener 'business_id' como una columna
    updated_df_reviews_yelp.reset_index(inplace=True)

    ## Cargamos los datos actualizados
    # Guardamos el DataFrame modificado de nuevo como un archivo Parquet en Storage
    bucket = client.get_bucket(bucket_yelp)
    nuevo_nombre_archivo = parquet_reviews_yelp
    nuevo_blob = bucket.blob(nuevo_nombre_archivo)
    nuevo_blob.upload_from_string(updated_df_reviews_yelp.to_parquet(), content_type='application/octet-stream')




    print('se realizó el ETL de manera correcta')