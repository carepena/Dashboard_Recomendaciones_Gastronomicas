# Tabla category_yelp

| Columna     | Descripción                  |
|-------------|------------------------------|
| category    | Descripción de la categoría. |
| category_id | ID único de la categoría.    |


# Tabla business_category_yelp

| Columna      | Descripción                          |
|--------------|--------------------------------------|
| business_id  | ID único del negocio.                |
| category_id  | ID único de la categoría asociada.   |

# Tabla tips_yelp

| Columna         | Descripción                           |
|-----------------|---------------------------------------|
| business_id     | ID único del negocio.                 |
| compliment_count| Número de cumplidos.                  |
| date            |Fecha de la revisión.          |
| text            |Texto de la revisión.                        |
| user_id         | ID único del usuario que dejó el tip. |



# Tabla business_attribute_yelp

| Columna      | Descripción                                    |
|--------------|------------------------------------------------|
| attribute_id | ID único del atributo.                         |
| business_id  | ID único del negocio asociado con el atributo. |
| value        | Valor del atributo.                            |

# Tabla attribute_yelp

| Columna     | Descripción                        |
|-------------|------------------------------------|
| attribute   | Nombre del atributo.               |
| attribute_id| ID único del atributo.             |

# Tabla business_horarios_google

| Columna   | Descripción                             |
|-----------|-----------------------------------------|
| close     | Hora de cierre.                         |
| day       | Día de la semana.                       |
| gmap_id   | ID único asociado con Google Maps.      |
| open      | Hora de apertura.                       |


# Tabla business_yelp

| Columna      | Descripción                                  |
|--------------|----------------------------------------------|
| address      | Dirección del negocio.                       |
| business_id  | ID único del negocio.                        |
| city_id      | ID  único de la ciudad donde se encuentra el negocio.|
| hours        | Informacion sobre los horarios de atencion para cada dia de la semana.           |
| latitude     | Coordenadas de latitud.         |
| longitude    | Coordenadas de longitud.        |
| name         | Nombre del negocio.                          |
| postal_code_id| ID  único del código postal del negocio.           |
| review_count |Número de reseñas escritas por usuarios sobre el negocio.             |
| stars        | Puntuación que le otorgan las reseñas en estrellas, en una escala del 1 al 5, con variaciones de 0,5.            |

# Tabla postal_code

| Columna     | Descripción                  |
|-------------|------------------------------|
| postal_code | Código postal.               |
| postal_code_id| ID único del código postal.|

# Tabla city

| Columna   | Descripción                     |
|-----------|---------------------------------|
| city      | 	Ciudad donde se encuentra el negocio.           |
| city_id   | ID único de la ciudad.          |
| country   | País donde se encuentra la ciudad.|
| state     | Abreviatura del estado en que se encuentra el negocio..|

# Tabla business_google

| Columna       | Descripción                              |
|---------------|------------------------------------------|
| address       | Dirección del negocio.                   |
| avg_rating    | Calificación promedio del negocio.       |
| city_id       | ID de la ciudad donde se encuentra el negocio.|
| description   | Descripción del negocio.                 |
| gmap_id       | ID único asociado con Google Maps.       |
| latitude      |Coordenadas de latitud.      |
| longitude     | Coordenadas de longitud.     |
| name          | Nombre del negocio.                      |
| num_of_reviews| Número de reseñas del negocio.           |
| postal_code_id| ID  único del código postal del negocio.        |


# Tabla business_misc_google

| Columna   | Descripción                                |
|-----------|--------------------------------------------|
| gmap_id   | ID único asociado con Google Maps.         |
| misc_id   | ID único del atributo del negocio.          |
| value     | Valor del atributo del negocio.             |

# Tabla misc_google

| Columna   | Descripción                     |
|-----------|---------------------------------|
| misc      | Caracteríscicas del comercio.  |
| misc_id   | ID único del atributo del negocio.     |

# Tabla business_category_google

| Columna   | Descripción                                     |
|-----------|-------------------------------------------------|
| category_id | ID único de la categoría.                     |
| gmap_id     | ID único asociado con Google Maps.            |

# Tabla category_google

| Columna     | Descripción                        |
|-------------|------------------------------------|
| category    | Nombre de la categoría.            |
| category_id | ID único de la categoría.          |


# Tabla users_yelp

| Columna            | Descripción                                               |
|--------------------|-----------------------------------------------------------|
| average_stars      | Promedio de calificación (en estrellas) otorgado a las reseñas que ha escrito el usuario.        |
| compliment_cool    | 	El número de elogios relacionados con ser "genial" que el usuario ha recibido.              |
| compliment_cute    |Número de elogios relacionados con ser "lindo" que el usuario ha recibido.            |
| compliment_funny   | Número de elogios relacionados con ser "gracioso" que el usuario ha recibido.            |
| compliment_hot     |Número de elogios o cumplidos relacionados con ser "caliente" que el usuario ha recibido.              |
| compliment_list    |Número de elogios relacionados con listas que el usuario ha recibido.             |
| compliment_more    | Número de elogios relacionados con "más" que el usuario ha recibido.             |
| compliment_note    | 	Número de elogios relacionados con notas o comentarios que el usuario ha recibido.             |
| compliment_photos  | Número de elogios relacionados con fotos que el usuario ha recibido.          |

# Tabla reviews_yelp

| Columna           | Descripción                                                |
|-------------------|------------------------------------------------------------|
| business_id       | ID único del negocio al que pertenece la reseña.           |
| compliment_count  | Cantidad de cumplidos recibidos por la reseña.             |
| date              | Fecha en que se publicó la reseña.                         |
| sentiment         | Sentimiento general de la reseña (positivo, negativo, etc.).|
| sentiment_score   | Puntuación del sentimiento de la reseña.                   |
| text              | Texto de la revisión.                                       |
| user_id           | ID único del usuario que escribió la reseña.               |


# Tabla reviews_google

| Columna          | Descripción                                                      |
|------------------|------------------------------------------------------------------|
| gmap_id          | ID único del lugar en Google Maps al que pertenece la reseña.    |                           |
| name             | Nombre del usuario que dejó la reseña.                           | |
| rating           | Calificación numérica de la reseña.                              |
| sentiment        | Sentimiento general de la reseña (positivo, negativo, etc.).     |
| sentiment_score  | Puntuación del sentimiento de la reseña, probablemente numérica. |
| state            | Abreviatura del estado en que se encuentra el negocio.                         |
| text             | Texto de la revisión.                                            |
