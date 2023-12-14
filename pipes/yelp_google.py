import requests
from google.cloud import storage
import pandas as pd
import pyparrow
import re

#configuramos el GCS
storage_client = storage.Client()
bucket = storage_client.bucket('actualizacion_de_archivos')



def save_df_to_gcs(data, context):

    # ---------------------------------- YELP -----------------------------------------------


    # Configuración inicial para Yelp
    YELP_API_KEY = 'j15mxo_y5xfdQj1mx34q4YZCGOnWCc7IH4mIdS1oz19MglzYCyL1gi0d32nEZT7a5jieUB4b1QyUnvAfcJf7rRE-hB0H_Pbovnw3BpFP6MhnSDbMUjWxgp8uDVJJZXYx'
    YELP_HEADERS = {'Authorization': f'Bearer {YELP_API_KEY}'}
    YELP_SEARCH_API_URL = 'https://api.yelp.com/v3/businesses/search'

    # Lista de estados para la búsqueda
    states = ['CA', 'FL', 'PA', 'IL', 'NJ']

    # Lista para almacenar los DataFrames de cada estado
    df_list_yelp = []

    for state in states:
        # Parámetros para la solicitud a la API de Yelp
        yelp_params = {
            'location': state,
            'limit': 49  # Número de negocios a retornar por estado
        }

        # Realiza la solicitud a la API de Yelp
        yelp_response = requests.get(YELP_SEARCH_API_URL, headers=YELP_HEADERS, params=yelp_params)

        # Verifica si la solicitud fue exitosa
        if yelp_response.status_code == 200:
            yelp_businesses = yelp_response.json()['businesses']
            # Crea un DataFrame con los datos recopilados
            df_state = pd.DataFrame(yelp_businesses)
            print(df_state.columns)
            # Selecciona solo las columnas que necesitas
            df_state = df_state[['id', 'alias', 'name', 'image_url', 'is_closed', 'url', 'review_count',
        'categories', 'rating', 'coordinates', 'transactions', 'price',
        'location', 'phone', 'display_phone', 'distance']]
            df_list_yelp.append(df_state)
        else:
            print(f'Error fetching data from Yelp API for {state}:', yelp_response.status_code)

    # Combina todos los DataFrames en uno solo
    df_yelp_combined_busines = pd.concat(df_list_yelp, ignore_index=True)


    # Nombre de archivo fijo
    filename_yelp_business = "business_yelp_actualizado.parquet"

    # Escribe a Cloud Storage
    df_yelp_combined_busines.to_parquet(filename_yelp_business)

    # Ruta del archivo 
    blob = bucket.blob("archivos_yelp_actualizados/" + filename_yelp_business)

    # Sube el archivo
    blob.upload_from_filename(filename_yelp_business)
    

    #REVIEWS

    # Configuración inicial
    HEADERS = {'Authorization': f'Bearer {YELP_API_KEY}'}

    # URL de la API de Yelp para obtener reseñas
    YELP_REVIEWS_URL = 'https://api.yelp.com/v3/businesses/{}/reviews'

    # Lista de IDs de negocios para los cuales queremos obtener reseñas
    business_ids = df_yelp_combined_busines['id'].tolist()

    # Lista para almacenar la información de las reseñas
    reviews_data = []

    # Función para obtener reseñas de un negocio
    def get_business_reviews(business_id):
        response = requests.get(YELP_REVIEWS_URL.format(business_id), headers=HEADERS)
        
        if response.status_code == 200:
            reviews = response.json().get('reviews', [])
            for review in reviews:
                # Crea un diccionario con los datos que quieres en tu DataFrame
                review_data = {
                    'user_id': review['user']['id'],
                    'business_id': business_id,
                    'author_name': review['user']['name'],
                    'rating': review['rating'],
                    'text': review['text'],
                    'time_created': review['time_created']
                }
                reviews_data.append(review_data)
        else:
            print(f'Error fetching data for business_id {business_id}: {response.status_code}')

    # Itera sobre la lista de business_ids y obtén las reseñas
    for bid in business_ids:
        get_business_reviews(bid)

    # Creamos un DataFrame con los datos recopilados
    df_reviews_yelp = pd.DataFrame(reviews_data)


    # Subimos a GCS

    # Nombre de archivo fijo
    filename_yelp_reviews = "reviews_yelp_actualizado.parquet"

    # Escribe a Cloud Storage
    df_reviews_yelp.to_parquet(filename_yelp_reviews)

    # Ruta del archivo 
    blob = bucket.blob("archivos_yelp_actualizados/" + filename_yelp_reviews)

    # Sube el archivo
    blob.upload_from_filename(filename_yelp_reviews)
    


    # ---------------------------------------- GOOGLE------------------------------------------------

    # Configuración inicial para Google Maps
    GOOGLE_MAPS_API_KEY = 'AIzaSyDoaNO3CgiYljGXwUatCiZH1bAbKyJk_Yk'
    GOOGLE_PLACES_API_URL = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json'

    # Coordenadas centrales aproximadas de cada estado
    state_coords = {
        'CA': '36.7783,-119.4179',
        'FL': '27.994402,-81.760254',
        'PA': '41.203322,-77.194525',
        'IL': '40.633125,-89.398528',
        'NJ': '40.058324,-74.405661'
    }

    # Lista para almacenar los DataFrames de cada estado
    df_list_google = []

    for state, coords in state_coords.items():
        # Parámetros para la solicitud a la API de Google Maps
        google_params = {
            'location': coords,
            'radius': 50000,  # Radio de búsqueda en metros
            'type': 'restaurant',  # Tipo de negocio a buscar
            'key': GOOGLE_MAPS_API_KEY,
            'limit': 50  # Número de negocios a retornar por estado
        }

        # Realiza la solicitud a la API de Google Maps
        google_response = requests.get(GOOGLE_PLACES_API_URL, params=google_params)

        # Verifica si la solicitud fue exitosa
        if google_response.status_code == 200:
            google_places = google_response.json()['results']
            # Crea un DataFrame con los datos recopilados
            df_state = pd.DataFrame(google_places)
            print(df_state.columns)
            # Selecciona solo las columnas que necesitas
            df_state = df_state[['business_status', 'geometry', 'name', 'opening_hours', 'place_id',
        'plus_code', 'price_level', 'rating', 'reference', 'types',
        'user_ratings_total', 'vicinity']]
            df_list_google.append(df_state)
        else:
            print(f'Error fetching data from Google Maps API for {state}:', google_response.status_code)

    # Combina todos los DataFrames en uno solo
    df_google_combined = pd.concat(df_list_google, ignore_index=True)


    
    # Subimos a GCS

    # Nombre de archivo fijo
    filename_google_business = "business_google_actualizado.parquet"

    # Escribe a Cloud Storage
    df_google_combined.to_parquet(filename_google_business)

    # Ruta del archivo 
    blob = bucket.blob("archivos_google_actualizados/" + filename_google_business)

    # Sube el archivo
    blob.upload_from_filename(filename_google_business)



    #REVIEWS

    # Configuración inicial
    # URL de la API de Google Places para obtener detalles
    GOOGLE_PLACES_DETAILS_URL = 'https://maps.googleapis.com/maps/api/place/details/json'

    # Función auxiliar para extraer el ID del autor de la URL
    def extract_author_id(author_url):
        match = re.search(r'/contrib/([0-9]+)/reviews', author_url)
        return match.group(1) if match else None


    # Lista de place_id para los cuales quieres obtener reseñas
    place_id = df_google_combined['place_id'].tolist()

    # Lista para almacenar la información de las reseñas
    reviews_data = []

    # Función para obtener reseñas de un place_id
    def get_place_reviews(place_id):
        params = {
            'place_id': place_id,
            'fields': 'reviews',
            'key': GOOGLE_MAPS_API_KEY
        }
        
        try:
        # Realiza la solicitud a la API de Google Places
            response = requests.get(GOOGLE_PLACES_DETAILS_URL, params=params)
            
            if response.status_code == 200:
                place_details = response.json()
                # Verifica si 'result' está en la respuesta y si contiene 'reviews'
                if 'result' in place_details and 'reviews' in place_details['result']:
                    for review in place_details['result']['reviews']:
                        author_id = extract_author_id(review['author_url'])
                        # Crea un diccionario con los datos que quieres en tu DataFrame
                        review_data = {
                            'place_id': place_id,
                            'user_id': author_id,  # Usamos el ID extraído en lugar de la URL completa
                            'author_name': review['author_name'],
                            'rating': review['rating'],
                            'text': review['text'],
                            'time': review['time']
                        }
                        reviews_data.append(review_data)
                    else:
                        print(f"No reviews found for place_id {place_id}")
                else:
                    print(f'Error fetching data for place_id {place_id}: {response.status_code}')
        except Exception as e:
            print(f'An error occurred: {e}')
    
    # Itera sobre la lista de place_ids y obtén las reseñas
    for pid in place_id:
        get_place_reviews(pid)

    # Crea un DataFrame con los datos recopilados
    df_google_reviews = pd.DataFrame(reviews_data)


    # Subimos a GCS

    # Nombre de archivo fijo
    filename_google_reviews = "reviews_google_actualizado.parquet"

    # Escribe a Cloud Storage
    df_google_reviews.to_parquet(filename_google_reviews)

    # Ruta del archivo 
    blob = bucket.blob("archivos_google_actualizados/" + filename_google_reviews)

    # Sube el archivo
    blob.upload_from_filename(filename_google_reviews)