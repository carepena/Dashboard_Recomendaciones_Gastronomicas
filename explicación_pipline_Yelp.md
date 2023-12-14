# Explicación del pipline automatizado de nuestros datos

Este proyecto implementa un proceso automatizado de ETL (Extracción, Transformación y Carga) para los datos de negocios y reseñas de Yelp y Google. Utiliza técnicas de computación en la nube y análisis de sentimientos para extraer insights valiosos.

*ahora entiendamos que es un pepline*

### ¿Qué es un Pipline?

Un "pipeline" en programación se refiere a una secuencia de pasos o etapas que se utilizan para procesar datos o realizar una serie de tareas de manera ordenada y eficiente. 

En un pipeline, cada etapa puede ser independiente y paralelizable, lo que permite un procesamiento eficiente y la posibilidad de escalar el sistema según sea necesario. Los pipelines son una herramienta poderosa para diseñar sistemas que manejen grandes volúmenes de datos o ejecuten tareas complejas de manera organizada y eficiente.


## Descripción
El script etl-yelp.py se ejecuta de forma automática en Google Cloud Platform en de forma programada todos los días, gracias a Google Pub/Sub y los guarda en nuestro almacenador de archivos Google Cloud Storage. Luego, de forma automatizada se extraen los últimos datos de negocios y reseñas de Yelp y Google almacenados en Google Cloud Storage en formato Parquet. Allí se realiza la limpieza y transformación de datos con Pandas, incluyendo análisis de sentimientos de las reseñas con TextBlob y NLTK. Finalmente, guarda los datos procesados en Parquet para usarlos en aplicaciones de BI, dashboards, sistemas de recomendación, etc. Todo este procedimiento se realiza ejecutandose los scripts de etl-yelp.py y etl-yelp.py.

### Tecnologías
Las principales tecnologías y librerías utilizadas son:

* **Python**: Lenguaje de programación interpretado y multiparadigma. Permite escribir código de forma sencilla y legible. Es muy popular para análisis de datos y machine learning.

* **Google Cloud Platform (GCP)**: Plataforma de nube pública de Google. Proporciona servicios de computación, almacenamiento, networking, big data y machine learning.

  * **Cloud Storage**: Servicio de almacenamiento de objetos en la nube. Permite almacenar archivos de cualquier tipo de forma redundante y escalable.
  * **Pub/Sub**: Servicio de mensajería asíncrona. Permite la comunicación entre componentes mediante el modelo publicador/suscriptor. Útil para coordinar trabajos y eventos.
* **Pandas**: Librería de Python para análisis de datos. Proporciona estructuras de datos (DataFrames) y herramientas para manipular y analizar datos.

* **PyArrow**: Librería para trabajar con columnar data sets eficientemente. Integra bien con Pandas.

* **TextBlob**: Librería para procesamiento de lenguaje natural. Permite realizar análisis de sentimientos.

* **NLTK (Natural Language Toolkit)**: Plataforma líder para trabajar con lenguaje humano en Python. Contiene recursos y módulos para tareas de NLP.



## Procedimiento

1. El script se ejecuta automáticamente de forma programada diaria, gracias a Pub/Sub. Extraemos los datos de las APIs y los almacenamos.

2. Se conecta a Cloud Storage y descarga los últimos archivos Parquet.

3. Lee el archivo Parquet en un DataFrame de Pandas.

4. Realiza limpieza y normalización de datos:

Convierte latitudes/longitudes a columnas separadas
Elimina campos innecesarios
Aplica análisis de sentimiento a las reseñas con TextBlob y NLTK.

6. Guarda los DataFrames procesados en Parquet en Cloud Storage.

De esta forma, mantenemos los datos analizados de Yelp y Google actualizados y listos para utilizar en aplicaciones de inteligencia de negocios y experiencia de cliente. El proceso automatizado garantiza que los insights extraídos reflejen los últimos datos de forma eficiente.

## Video

Creamos un [video](https://drive.google.com/file/d/1lrnQAMO9Aa_GPigfIoAYQdb_JjyEDBwJ/view?usp=drive_link) en donde corremos una "*muestra*" de nuestro codigo. También verificamos que nuestros archivos estan siendo modificados.

### Archivos

* [`pipes`](https://github.com/Hotcer/Spartans-Project/tree/master/pipes): Es una carpeta que contiene los **scripts** que se ejecuta para nuestra automatización de extracción de datos, limpieza, transformaciones y actualizaciones.
* [`Etl-Explicación`](https://docs.google.com/document/d/19tRJGJ6W5Q_1I1Y5kvJ2ZfXR67C12KnO/edit?usp=drive_link&ouid=111305273898691886674&rtpof=true&sd=true): Es un archivo word que explica más en detalle el trabajo y proceso de nuestro pipline automatizado,
