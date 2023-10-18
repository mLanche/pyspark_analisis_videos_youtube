**PYSPARK: TRANSFORMACIÓN Y ANÁLISIS DE DATOS DE VÍDEOS DE YOUTUBE**

Originalmente este proyecto ha sido ejecutado en Google Cloud en la plataforma de Dataproc en Jupyter Lab. 
Con la intención de subirlo a GitHub y que se pueda probar en local, he creado una sessión de Spark en local.

En el siguiente proyecto se analizan diferentes datasets relativos a los vídeos de YouTube de Canadá, EEUU y Gran Bretaña.
Para leer, transformar, limpiar y analizar los datos utilizo el framework de Spark.


**1**: Lectura de los datos de cada país por separado y luego concateno los 3 datasets en uno solo, usando la función .union()
También se comprueba el Schema.

**2**: Casteo el tipo de dato de las columnas, entre ellas diferentes fechas con diferentes formatos. 

También elimino nulos y finalmente cacheo el dataframe porque voy a utilizarlo reiteradas veces. Compruebo el esquema y veo que todo está correcto.

**3**: Creo un dataframe a partir del anterior en el cuál añado más información:

    - Hago un replace del id de la categoría, por el nombre de la categoría. 
    - Calculo el número de días que pasaron desde la publicación hasta que se volvió viral el vídeo
    - Obtengo el día concreto en el que el vídeo se hace viral partir de la fecha de publicación.
    - Transformo la columna tags para quedarme con un array de strings

**4**: Quiero averiguar cuátos días es vireal un vídeo en cada país , para ello:

    - Creo una ventana sobre vídeo_id y país
    - Creo un dataframe y obtengo en minimo y el máximo de trending_date sobre la ventana creada
    - Obtengo el Nº de días que ha sido viral un video a raíz de la diferencia entre ese máximo y mínimo
    - Elimino las columnas max y min, y también los duplicadoss generados sobre vídeo y país.
    - Cacheo el DataFrame porque voy a utilizarlo varias veces

**5**:  Averiguo la media de los días en los que un vídeo es viral en cada país, según su categoría:

    - Agrupo por categoría
    - Pivoto por país 
    - Calculo la media sobre la columna "diás_viral_país" que obtuve en el punto anterior.
Ahora quiero saber cuantos vídeos hay cada día de la semana según la categoría:

    - Agrupo por "categoría_id" 
    - Pivoto por la columna "diasemana", y luego pongo que la cabecera sean los días que he definido en una lista 
    - Calculo el nº de vídeos distintos usando video_id

**6**: Quiero analizar cuantas veces aparece el substrig "music" en la columna "tags":

    - Defino una función en python para sumar 1 cuando aparezca "music"
    - Convierto esa función a una UDF para utilizarla con spark
    - Creo una columna "ocurrencias_music" y aplico la UDF

Ahora quiero saber la ocurrencia media según  la categoría y el país:

    - Creo una ventana para agrupar por categoría y país
    - Creo la columna "ocurrencia_media" calculo la media sobre  la columna anterior y sobre la ventana 
    - Calculo el tanto por ciento en qué medida el número de apariciones de "music" está por encima o por debajo de la media de los vídeos de su misma categoría y país.



