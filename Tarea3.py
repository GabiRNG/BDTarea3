# Importamos librerías necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'

# Carga el conjunto de datos
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# Imprimimos el esquema del DataFrame
print("Esquema del DataFrame:")
df.printSchema()

# Muestra las primeras filas del DataFrame
print("Primeras filas del DataFrame:")
df.show()

# Realizar operaciones de limpieza
# 1. Eliminar filas con valores nulos
df_cleaned = df.na.drop()

# 2. Reemplazar valores nulos en la columna 'VALOR' con un valor por defecto (0)
df_cleaned = df_cleaned.na.fill({'VALOR': 0})

# 3. Convertir columnas a tipos de datos apropiados si es necesario
# Por ejemplo, si 'VIGENCIADESDE' y 'VIGENCIAHASTA' deben ser fechas
df_cleaned = df_cleaned.withColumn('VIGENCIADESDE', F.to_date('VIGENCIADESDE', 'yyyy-MM-dd'))
df_cleaned = df_cleaned.withColumn('VIGENCIAHASTA', F.to_date('VIGENCIAHASTA', 'yyyy-MM-dd'))

# Análisis exploratorio de datos (EDA)
# 1. Estadísticas básicas
print("Estadísticas básicas del DataFrame limpio:")
df_cleaned.summary().show()

# 2. Filtrar por valores y seleccionar columnas específicas
print("Días con valor mayor a 5000:")
dias = df_cleaned.filter(F.col('VALOR') > 5000).select('VALOR', 'VIGENCIADESDE', 'VIGENCIAHASTA')
dias.show()

# 3. Agrupación y conteo
print("Conteo de registros agrupados por 'VIGENCIAHASTA':")
grouped_df = df_cleaned.groupBy('VIGENCIAHASTA').count()
grouped_df.show()

# 4. Cálculo de la media y la suma de la columna 'VALOR'
print("Media y suma de la columna 'VALOR':")
df_cleaned.agg(F.avg('VALOR').alias('Media Valor'), F.sum('VALOR').alias('Suma Valor')).show()

# Almacenar los resultados procesados
# Guardar el DataFrame limpio en HDFS como un archivo CSV
output_path_cleaned = 'hdfs://localhost:9000/Tarea3/cleaned_data.csv'
df_cleaned.write.format('csv').option('header', 'true').save(output_path_cleaned)

# Guardar el DataFrame de agrupación en HDFS
output_path_grouped = 'hdfs://localhost:9000/Tarea3/grouped_data.csv'
grouped_df.write.format('csv').option('header', 'true').save(output_path_grouped)

# Finalizar la sesión de Spark
spark.stop()
