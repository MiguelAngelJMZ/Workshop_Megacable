# Databricks notebook source
# MAGIC %md
# MAGIC ![Problemas Data Engineering](images/Databricks_Logo.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Engineering con Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Introducción
# MAGIC
# MAGIC En esta parte, vamos a sumergirnos de lleno en el mundo de la Ingeniería de Datos. Veremos que, aunque es un campo fundamental, a menudo presenta desafíos como la fragmentación tecnológica, la complejidad en la calidad de los datos y las trabas operativas.
# MAGIC
# MAGIC A lo largo de este notebook, descubrirán cómo Databricks se posiciona como una solución única, centralizando todo el proceso en un solo lugar. Usaremos herramientas clave como Lakeflow Declarative Pipelines para automatizar nuestros flujos, Auto Loader para gestionar los datos de manera incremental y ETL con Spark para aplicar las transformaciones necesarias.
# MAGIC
# MAGIC El objetivo es que entiendan cómo construir pipelines de datos eficientes y escalables, superando los obstáculos que la ingeniería de datos presenta comúnmente. ¡Manos a la obra!

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 El Desafío de los Pipelines de Datos
# MAGIC Construir y mantener pipelines de datos confiables es una tarea compleja, pero ¿por qué?
# MAGIC
# MAGIC El desafío principal es la diversidad. Los datos provienen de múltiples fuentes y formatos, lo que exige un gran esfuerzo para limpiarlos, transformarlos e integrarlos correctamente.
# MAGIC
# MAGIC Además de esto, existen otros dos factores clave que añaden complejidad:
# MAGIC
# MAGIC - Calidad de los datos: Mantener una calidad alta a lo largo de todo el pipeline es crucial, y a menudo, difícil de lograr.
# MAGIC
# MAGIC - Complejidad operativa: Integrar el streaming en tiempo real y la orquestación de datos introduce una capa adicional de dificultad que requiere un manejo cuidadoso.
# MAGIC
# MAGIC En resumen, no es solo mover datos de un lugar a otro, es gestionar la diversidad, la calidad y la complejidad operativa de principio a fin.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="images/Problemas_Data_Engineering.png" alt="Descripción" width="800">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 La Solución de Databricks para la Ingeniería de Datos
# MAGIC
# MAGIC La ingeniería de datos se divide en cuatro componentes principales. Con la plataforma de Databricks, cada uno se integra a la perfección, simplificando el proceso de principio a fin.
# MAGIC
# MAGIC 1. **Ingesta de Datos** <br>
# MAGIC Aquí es donde recibimos datos de diversas fuentes, sin importar su formato. Para esta tarea, puedes usar Lakeflow Connect, que ofrece una variedad de conectores a las bases de datos y aplicaciones empresariales más comunes, facilitando la integración. A través de Auto Loader puedes cargar datos de forma autmatizada cada vez que nuevos archivos llegen a tu lugar de almacenamiento.
# MAGIC
# MAGIC 2. **Transformación de Datos** <br>
# MAGIC Una vez ingeridos, limpiamos y transformamos los datos para hacerlos útiles. Con Databricks, puedes usar Lakeflow Declarative Pipelines para construir pipelines ETL de manera simple y con un lenguaje declarativo. Para lógica más compleja, tienes la flexibilidad de usar PySpark.
# MAGIC
# MAGIC 3. **Orquestación** <br>
# MAGIC Automatizamos y gestionamos todo el flujo de trabajo para garantizar un proceso eficiente y confiable. Lakeflow Jobs te permite crear una lógica cíclica para tu flujo de datos, asegurando que tus pipelines se ejecuten de manera organizada y programada.
# MAGIC
# MAGIC 4. **Procesamiento en Tiempo Real** <br>
# MAGIC Cuando necesitas insights al instante, esta capa es crucial. Con Structured Streaming, puedes procesar datos con una baja latencia, lo que te permite cumplir con los SLAs (acuerdos de nivel de servicio) más exigentes.
# MAGIC
# MAGIC En resumen, Databricks une estos componentes en un solo lugar, permitiendo que tus datos fluyan de manera fluida y automatizada desde su origen hasta su uso final.

# COMMAND ----------

# MAGIC %md
# MAGIC <div align="center">
# MAGIC   <img src="images/Solucion_Data_Engineering.png" alt="Descripción" width="800">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Setup
# MAGIC
# MAGIC Ejecuta la siguiente celda para fijar tu ambiente de trabajo para este curso

# COMMAND ----------

# MAGIC %run ./setup/SetupWorkshop

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tabla de Codigo Postal
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Lakeflow Daclarative Piopelines
# MAGIC <br></br>
# MAGIC <div align="center">
# MAGIC   <img src="images/Declarative_Pipelines1.png" alt="Descripción" width="800">
# MAGIC </div>
# MAGIC
# MAGIC <br></br>
# MAGIC <div align="center">
# MAGIC   <img src="images/Declarative_Pipelines2.png" alt="Descripción" width="800">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1.1 Copia de datos
# MAGIC
# MAGIC Ejecuta la siguiente celda para realizar la copia de datos desde el repositorio de GitHub hacia nuestro volúmen en Databricks

# COMMAND ----------

# MAGIC %run ./setup/Copia_CodigoPostal

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1.2 Construye un Declarative Pipeline
# MAGIC
# MAGIC Ejecuta la siguiente línea de código, accede al enlace y ejecuta el Pipeline

# COMMAND ----------

# MAGIC %run ./setup/Pipeline_Codigo_Postal

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Lakeflow Designer
# MAGIC
# MAGIC Nos complace anunciar Lakeflow Designer, un generador de pipelines sin código, basado en IA y totalmente integrado con la plataforma de inteligencia de datos de Databricks. Con un lienzo visual y una interfaz de lenguaje natural integrada, Designer permite a los analistas de negocio crear pipelines de producción escalables y realizar análisis de datos sin escribir una sola línea de código, todo en un único producto unificado.
# MAGIC
# MAGIC Cada pipeline creado en Designer crea un pipeline declarativo de Lakeflow. Los ingenieros de datos pueden revisar, comprender y mejorar estos pipelines sin cambiar de herramienta ni reescribir la lógica, ya que se trata del mismo estándar ANSI SQL utilizado en Databricks. Esto permite a los usuarios sin código participar en el trabajo con datos sin generar sobrecarga adicional para los ingenieros.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <br></br>
# MAGIC <div align="center">
# MAGIC   <img src="images/Lakeflow-designer.gif" alt="Descripción" width="1000">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tabla Clientes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1. Ingesta de datos con Auto Loader (Fuente -> Bronze)
# MAGIC
# MAGIC El componente Auto Loader de Databricks permite la ingesta de nuevos archivos de datos de manera incremental y eficiente a medida que llegan al almacenamiento en la nube, sin necesidad de configuraciones adicionales. Este mecanismo es especialmente útil para procesar millones de archivos en tiempo real, apoyando formatos como JSON, CSV, PARQUET, entre otros.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.1.1 Realiza una copia de los datos a un Volumen de Databricks
# MAGIC
# MAGIC Ejecuta la siguiente celda para realizar la copia de datos desde el repositorio de GitHub hacia nuestro volúmen en Databricks

# COMMAND ----------

# MAGIC %run ./setup/Copia_Clientes1

# COMMAND ----------

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv") 
    .option("cloudFiles.schemaLocation", schema_location)
    .load(input_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.1.2 Ejecuta una consulta sobre los datos en Streaming
# MAGIC
# MAGIC Utiliza el comando writeStream para escribir en memoria los datos streaming y posteriormente desplegar

# COMMAND ----------

query = df.writeStream \
    .format("memory") \
    .queryName("streaming_df") \
    .trigger(availableNow=True) \
    .start()  # Esto inicia el stream

query.awaitTermination(timeout=5)  # Esto espera a que termine el stream

streaming_df = spark.sql("SELECT * FROM streaming_df")  # Esto obtine la tabla en memoria

display(streaming_df.limit(5))  # Esto despliega la tabla en memoria

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.1.3 Cambia el nombre de las columnas para acoplarse a la tabla base y el formato delta
# MAGIC
# MAGIC Las tablas delta no permiten cierto tipo de carácters, por lo que se debe cambiar el nombre de las columnas para que sean compatibles con la tabla delta.
# MAGIC
# MAGIC * Espacio (' ') 
# MAGIC * Coma (,)
# MAGIC * Punto y coma (;)
# MAGIC * Llaves ({})
# MAGIC * Paréntesis (())
# MAGIC * Nueva línea (\n)
# MAGIC * Tabulación (\t)
# MAGIC * Signo igual (=)

# COMMAND ----------

col_renames = {
    "Customer ID": "id_cliente",
    "Gender": "genero",
    "Age": "edad",
    "Married": "casado",
    "Number of Dependents": "dependientes",
    "City": "ciudad",
    "Zip Code": "codigo_postal",
    "Latitude": "latitud",
    "Longitude": "longitud",
    "Number of Referrals": "numero_referidos",
    "Tenure in Months": "tenencia_meses",
    "Offer": "oferta",
    "Phone Service": "servicio_telefono",
    "Avg Monthly Long Distance Charges": "prom_mensual_larga_distancia",
    "Multiple Lines": "multiples_lineas",
    "Internet Service": "servicio_internet",
    "Internet Type": "tipo_internet",
    "Avg Monthly GB Download": "prom_mensual_gb_descarga",
    "Online Security": "seguridad_online",
    "Online Backup": "backup_online",
    "Device Protection Plan": "plan_proteccion_dispositivos",
    "Premium Tech Support": "soporte_premium",
    "Streaming TV": "tv_streaming",
    "Streaming Movies": "peliculas_streaming",
    "Streaming Music": "musica_streaming",
    "Unlimited Data": "datos_ilimitados",
    "Contract": "tipo_contrato",
    "Paperless Billing": "recibo_electronico",
    "Payment Method": "tipo_pago",
    "Monthly Charge": "cargo_mensual",
    "Total Charges": "cargos_totales",
    "Total Refunds": "total_reembolso",
    "Total Extra Data Charges": "total_cargos_extra",
    "Total Long Distance Charges": "total_cargos_larga_distancia",
    "Total Revenue": "total_ingreso",
    "Customer Status": "estatus_cliente",
    "Churn Category": "churn_cliente",
    "Churn Reason": "churn_razon"
}

# COMMAND ----------

from functools import reduce

df_renamed = reduce(
    lambda df, col: df.withColumnRenamed(col, col_renames[col]),
    col_renames,
    df
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.1.4 Define una tarea Auto Loader
# MAGIC
# MAGIC Para esta tarea, vamos a procesar los datos de Cliente que están en el Volumen `workshop_megacable.<nombre_usuario>_raw_data` en la carpeta **clientes**
# MAGIC
# MAGIC > Toma en cuenta lo siguiente:
# MAGIC 1. input_path: ruta del volumen/almacenamiento donde se guardan los archivos a procesar
# MAGIC 2. checkpoint_path: es el directorio donde se guarda el "estado de progreso" de la ingesta en modo streaming. Gracias a esto, Auto Loader ofrece la garantía de procesar los archivos exactamente una vez (exactly-once processing).
# MAGIC 3. table_name: nombre de la tabla destino donde se guarda la información de **input_path**
# MAGIC 4. schema_location: almacena la información sobre la evolución del esquema de los datos a lo largo del tiempo

# COMMAND ----------

query = (
    df_renamed.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("cloudFiles.schemaLocation", schema_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .outputMode("append") 
    .toTable(table_name)  # This writes to a managed table
)

# COMMAND ----------

sql_command = f"""SELECT * FROM workshop_megacable.{usuario}_bronze.clientes"""

display(sql_command)

# COMMAND ----------

# MAGIC %run ./setup/Copia_Clientes2

# COMMAND ----------

query = (
    df_renamed.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("cloudFiles.schemaLocation", schema_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .outputMode("append") 
    .toTable(table_name)  # This writes to a managed table
)

# COMMAND ----------

sql_command = f"""SELECT * FROM workshop_megacable.{usuario}_bronze.clientes"""

display(sql_command)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Transforma tus datos (bronze -> silver)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.1 Corrección de esquema
# MAGIC
# MAGIC En la capa Bronze, los datos suelen llegar con tipos de datos genéricos o incorrectos. Antes de aplicar cualquier otra transformación y mover la información a la capa Silver, es fundamental corregir el esquema.
# MAGIC
# MAGIC Este paso inicial nos permite asegurarnos de que cada columna tenga el tipo de dato correcto, tal como lo requiere el negocio. De esta forma, garantizamos la integridad de los datos y facilitamos los análisis posteriores.

# COMMAND ----------

bronze_clientes = spark.read.table(table_name)

display(bronze_clientes.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, DoubleType, BooleanType

# Diccionario: columna -> nuevo tipo
type_mapping = {
    "id_cliente": StringType(),
    'genero': StringType(),
    'edad': IntegerType(),
    'casado': StringType(),
    'dependientes': IntegerType(),
    'ciudad': StringType(),
    'codigo_postal': IntegerType(),
    'latitud': DoubleType(),
    'longitud': DoubleType(),
    'numero_referidos': IntegerType(),
    'tenencia_meses': IntegerType(),
    'oferta': StringType(),
    'servicio_telefono': StringType(),
    'prom_mensual_larga_distancia': DoubleType(),
    'multiples_lineas': StringType(),
    'servicio_internet': StringType(),
    'tipo_internet': StringType(),
    'prom_mensual_gb_descarga': DoubleType(),
    'seguridad_online': StringType(),
    'backup_online': StringType(),
    'plan_proteccion_dispositivos': StringType(),
    'soporte_premium': StringType(),
    'tv_streaming': StringType(),
    'peliculas_streaming': StringType(),
    'musica_streaming': StringType(),
    'datos_ilimitados': StringType(),
    'tipo_contrato': StringType(),
    'recibo_electronico': StringType(),
    'tipo_pago': StringType(),
    'cargo_mensual': DoubleType(),
    'cargos_totales': DoubleType(),
    'total_reembolso': DoubleType(),
    'total_cargos_extra': DoubleType(),
    'total_cargos_larga_distancia': DoubleType(),
    'total_ingreso': DoubleType(),
    'estatus_cliente': StringType(),
    'churn_cliente': StringType(),
    'churn_razon': StringType()
}

bronze_clientes = bronze_clientes.select([
    col(c).cast(type_mapping[c]) if c in type_mapping else col(c)
    for c in bronze_clientes.columns
])

# COMMAND ----------

display(bronze_clientes.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.2 Eliminación de duplicados
# MAGIC
# MAGIC Para este paso, nos enfocaremos en la columna id_cliente, que funciona como el identificador único de nuestra tabla.
# MAGIC
# MAGIC Dado que las filas duplicadas tienen los mismos valores en todas las columnas, solo necesitamos quedarnos con una de ellas. Nos basaremos únicamente en id_cliente para identificar y eliminar los registros duplicados, conservando una única entrada por cada cliente.

# COMMAND ----------

bronze_clientes = bronze_clientes.dropDuplicates(['id_cliente'])

# COMMAND ----------

bronze_clientes.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.3 Limpieza de outliers
# MAGIC
# MAGIC Para este paso, nos centraremos en la columna numero_referidos.
# MAGIC
# MAGIC Según las reglas de negocio, eliminaremos todos los registros que se encuentren fuera del rango del 5% al 95% de la distribución normal de los datos. Esta acción nos ayudará a eliminar los valores atípicos y a trabajar con un conjunto de datos más consistente y representativo.

# COMMAND ----------

# Paso 1: Calcular los percentiles 5 y 95
bounds = bronze_clientes.approxQuantile("numero_referidos", [0.05, 0.98], 0.01)
p98 = bounds[1]

# COMMAND ----------

display(bronze_clientes.filter((bronze_clientes["numero_referidos"] >= p98)))

# COMMAND ----------

# Paso 2: Filtrar registros dentro de los percentiles
bronze_clientes = bronze_clientes.filter((bronze_clientes["numero_referidos"] <= p98))

# COMMAND ----------

bronze_clientes.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.4 Validar Valores con Catálogo
# MAGIC En este paso, nos enfocaremos en la columna codigo_postal.
# MAGIC
# MAGIC Una regla de negocio común es validar que los valores de una columna existan en un catálogo maestro. Para esta transformación, nos aseguraremos de que todos los registros que pasen a la capa Silver tengan un codigo_postal que coincida con uno de los valores de la tabla codigo_postal.
# MAGIC
# MAGIC Todos los registros que no cumplan con esta validación serán eliminados.

# COMMAND ----------

# Leamos la tabla Código Postal
codigo_postal = spark.read.table(f"workshop_megacable.{usuario}_silver.codigo_postal")

# COMMAND ----------

# Eliminemos las columnas que no nos interesan
cols_clients = [c for c in bronze_clientes.columns if c not in [ "_rescued_data"]]
cols_codigo = [c for c in codigo_postal.columns if c not in [ "_rescued_data"]]

# Unimos las tablas
bronze_clientes = bronze_clientes.select(cols_clients).join(codigo_postal.select(cols_codigo), bronze_clientes["codigo_postal"] == codigo_postal["id_codigo_postal"], "left")

# COMMAND ----------

bronze_clientes.count()

# COMMAND ----------

# Descartamos los registros cuyo código postal no existe
bronze_clientes = bronze_clientes.filter(bronze_clientes["id_codigo_postal"].isNotNull())

# COMMAND ----------

bronze_clientes.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.5 Normalización de Valores
# MAGIC Para esta transformación, nos enfocaremos en las columnas genero y tipo_pago.
# MAGIC
# MAGIC A menudo, los datos del cliente contienen errores o inconsistencias en sus valores. Para corregir esto, aplicaremos las siguientes reglas de normalización:
# MAGIC
# MAGIC   * genero: Transformaremos las abreviaciones a su valor completo. Cambiaremos "F" por "Female" y "M" por "Male".
# MAGIC
# MAGIC   * tipo_pago: De igual manera, convertiremos las abreviaciones a su descripción completa. "BW" se transformará en "Bank Withdrawal", "CC" en "Credit Card" y "MC" en "Mailed Check".

# COMMAND ----------

display(bronze_clientes.limit(50))

# COMMAND ----------

bronze_clientes = bronze_clientes.replace({"F": "Female", "M": "Male"}, subset=["genero"]) \
  .replace({'BW': 'Bank Withdrawal', 'CC': 'Credit Card', 'M': 'Mailed Check'})

# COMMAND ----------

display(bronze_clientes.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.3 Creación de tablas Gold

# COMMAND ----------

silver_table = spark.read.table(f'workshop_megacable.{usuario}_silver.clientes')

# COMMAND ----------

silver_table.write.mode("overwrite").saveAsTable(f"workshop_megacable.{usuario}_gold.clientes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. BONUS: Utiliza Jobs para orquestar Notebooks (y otros componentes)
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Recap
# MAGIC
# MAGIC Hemos llegado al final de nuestro segundo notebook sobre Ingeniería de Datos. Repasemos los conceptos clave que cubrimos:
# MAGIC
# MAGIC 1. **Los desafíos de la ingeniería de datos:** Vimos que la ingeniería de datos presenta retos significativos debido a la fragmentación tecnológica, la complejidad en la calidad de los datos y las trabas operativas.
# MAGIC
# MAGIC 2. **La solución de Databricks:** Descubrimos cómo Databricks resuelve estos problemas centralizando la ingeniería de datos en una sola plataforma. Esto permite a los equipos trabajar de forma eficiente y escalable.
# MAGIC
# MAGIC 3. **Lakeflow Declarative Pipelines:** Usamos la interfaz de usuario y código para crear un pipeline declarativo, llevando datos desde la fuente hasta la capa Gold.
# MAGIC
# MAGIC 4. **Auto Loader:** Aprendimos a usar Auto Loader para detectar automáticamente nuevos archivos en el storage, lo que nos permite procesar los datos de manera incremental y moverlos a las capas posteriores (Bronze, Silver y Gold).
# MAGIC
# MAGIC 5. **ETL con Spark:** Finalmente, usamos Spark para construir un proceso ETL completo, aplicando lógica de transformación para preparar nuestros datos. También aprendimimos a orquestar este ETL a través de Lakeflow Jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC _____

# COMMAND ----------

# MAGIC %md
# MAGIC _______

# COMMAND ----------

# MAGIC %md
# MAGIC # ¡¡¡FELICIDADES!!!
# MAGIC
# MAGIC Haz acabado la  primera parte de este laborario práctico, ahora sabes que mediante databricks de Databricks puedes realizar ETL's de una forma fácil y escalable.
# MAGIC
# MAGIC La siguiente parte del laborario se centra en Data Visualization. Vamos a ello:
# MAGIC
# MAGIC [Data Visualization]($./03. Data Visualizaciones)
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="images/Cat_Im_Cooking.gif" alt="Descripción" width="400">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>