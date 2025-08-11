# Databricks notebook source
# MAGIC %md
# MAGIC ![Problemas Gobernanza de Datos](images/Databricks_Logo.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Governance con Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Introducción
# MAGIC
# MAGIC Hoy vamos a sumergirnos en un tema crucial, pero a menudo desafiante, en el mundo de los datos: la gobernanza. A lo largo de este laboratorio, veremos por qué la gobernanza de datos es tan difícil en la práctica y cómo Databricks nos ofrece una solución integral a través de Unity Catalog.
# MAGIC
# MAGIC Verán que construiremos un entorno real desde cero, creando esquemas y tablas, y aplicaremos técnicas clave como el uso de etiquetas (tags) y el control de acceso a nivel de fila y columna.
# MAGIC
# MAGIC Todo esto lo haremos con un objetivo claro: entender y aplicar las herramientas que Databricks nos da para centralizar la gestión y garantizar que nuestros datos sean confiables, seguros y fáciles de usar.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gobernanza de datos
# MAGIC
# MAGIC La gobernanza es el conjunto de prácticas, políticas y herramientas que garantizan que los datos y los activos de inteligencia artificial de una organización sean gestionados de forma segura, accesible, comprensible y conforme a regulaciones y lineamientos internos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 El Desafío de la Gobernanza de Datos
# MAGIC
# MAGIC Todos sabemos que gobernar los datos es importante, pero la realidad es que la fragmentación lo complica.
# MAGIC
# MAGIC Los datos están dispersos en múltiples lugares y formatos (bases de datos, data lakes, Delta Lake, Iceberg, etc.), además de existir otros activos como modelos de IA y dashboards. Esto crea silos y desafíos clave:
# MAGIC
# MAGIC - Gobernanza fragmentada: No podemos gestionar el acceso y el linaje de manera unificada.
# MAGIC
# MAGIC - Falta de conectividad: Los usuarios no pueden compartir datos y modelos entre herramientas.
# MAGIC
# MAGIC - Falta de inteligencia: Es difícil para los usuarios encontrar y entender los datos, dependiendo demasiado de expertos.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="images/Problemas_Gobernanza.png" alt="Descripción" width="800">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 La solución de Databricks para la Gobernanza de Datos
# MAGIC
# MAGIC Unity Catalog de Databricks es la primera solución de gobernanza de datos e IA unificada y abierta.
# MAGIC
# MAGIC A diferencia de los catálogos tradicionales, que solo gestionan el acceso a tablas, Unity:
# MAGIC
# MAGIC 1. Funciona con cualquier formato abierto.
# MAGIC
# MAGIC 2. Gobierna todos los activos de datos e IA.
# MAGIC
# MAGIC 3. Centraliza la gestión de acceso, linaje y auditoría en un solo sistema.
# MAGIC
# MAGIC Es más que un simple catálogo; es la base de la inteligencia de datos, abierto a cualquier herramienta o fuente externa.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="images/Solucion_Gobernanza.png" alt="Descripción" width="800">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Setup
# MAGIC
# MAGIC Ejecuta la siguiente celda para fijar tu ambiente de trabajo para este curso
# MAGIC

# COMMAND ----------

# MAGIC %run ./setup/SetupWorkshop

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Crear contenedores de datos en Unity Catalog
# MAGIC
# MAGIC En esta sección, exploraremos cómo crear contenedores de datos y objetos en Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Catálogos
# MAGIC
# MAGIC Un catálogo en Unity Catalog el contenedor de nivel superior que organiza y agrupa esquemas, tablas y otros objetos de datos dentro de la plataforma Databricks. Cada catálogo actúa como un espacio lógico donde se definen los permisos y la estructura de los datos, permitiendo segmentar y administrar de manera separada los diferentes conjuntos de información de una organización.
# MAGIC
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="images/Levels_Unity_Catalog.png" alt="Descripción" width="800">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.1.1 Crear Catálogos
# MAGIC Para crear un catálogo, utiliza la instrucción `CREATE CATALOG <nombre_del_catálogo>`. La palabra clave `IF NOT EXISTS` evitará errores si el nombre del catálogo ya existe.
# MAGIC
# MAGIC Para este laboratorio vamos a crear el catálogo llamado **workshop_megacable**

# COMMAND ----------

#spark.sql("CREATE CATALOG IF NOT EXISTS workshop_megacable")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.1.2 Metadatos del Catálogo
# MAGIC Utiliza el comando `DESCRIBE CATALOG <nombre_del_catalogo>` para consultar los metadatos básicos del cátalogo como Nombre, Comentarios y Propietarios. En adición, utiliza el comando `EXTENDED` para consultar metadatos adicionales del catálogo.

# COMMAND ----------

sql_command = f"""DESCRIBE CATALOG workshop_megacable"""

display(spark.sql(sql_command))

# COMMAND ----------

sql_command = f"""DESCRIBE CATALOG EXTENDED workshop_megacable"""

display(spark.sql(sql_command))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Schemas
# MAGIC
# MAGIC Un esquema en Unity Catalog es similar al de los esquemas o bases de datos con los que probablemente ya estás familiarizado. Los esquemas contienen objetos de datos como tablas y vistas, pero también pueden contener funciones y modelos de aprendizaje automático/inteligencia artificial.
# MAGIC
# MAGIC Para este taller, usaremos la arquitectura del esquema medallón, un enfoque que organiza tus datos en diferentes etapas de procesamiento. Crearemos un esquema para cada una de ellas:
# MAGIC
# MAGIC - **Bronze** (`workshop_megable.<nombre_usuario>.bronze`): Aquí recibiremos los datos en crudo, sin ninguna modificación. Es nuestra zona de aterrizaje.
# MAGIC
# MAGIC - **Silver** (`workshop_megable.<nombre_usuario>.silver`): En esta capa, aplicaremos transformaciones y reglas de negocio. Los datos ya estarán limpios y estructurados, siguiendo un modelo de datos definido.
# MAGIC
# MAGIC - **Gold** (`workshop_megable.<nombre_usuario>.gold`): Esta es la capa final, lista para el consumo de los usuarios. Aquí los datos ya están preparados para análisis y reportes.
# MAGIC
# MAGIC
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="images/Arquitectura_Medallon.png" alt="Descripción" width="800">
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2.1 Crear Esquema
# MAGIC Para crear un esquema, utiliza la instrucción CREATE SCHEMA `<nombre_del_catálogo>.<nombre_del_esquema>`. La palabra clave IF NOT EXISTS evitará errores si el nombre del esquema ya existe en ese catálogo.

# COMMAND ----------

sql_command_bronze = f"""CREATE SCHEMA IF NOT EXISTS workshop_megacable.{usuario}_bronze;"""
sql_command_silver = f"""CREATE SCHEMA IF NOT EXISTS workshop_megacable.{usuario}_silver;"""
sql_command_gold  = f"""CREATE SCHEMA IF NOT EXISTS workshop_megacable.{usuario}_gold;"""

display(spark.sql(sql_command_bronze))
display(spark.sql(sql_command_silver))
display(spark.sql(sql_command_gold))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2.2 Esquemas contenidos en el catálogo
# MAGIC
# MAGIC Utiliza el comando `SHOW SCHEMAS IN <nombre_catalogo>` para mostrar todos los esquemas contenidos en ese catálogo.

# COMMAND ----------

sql_command = f"""SHOW SCHEMAS IN workshop_megacable"""

display(spark.sql(sql_command))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2.3 Metadatos del esquema
# MAGIC
# MAGIC Utiliza el comando `DESCRIBE SCHEMA <nombre_del_catalogo>.<nombre_esquema>` para consultar los metadatos básicos del cátalogo como Nombre, Comentarios y Propitarios. En adición, utiliza el comando `EXTENDED` para consultar metadatos adicionales del catálogo.

# COMMAND ----------

sql_command = f"""DESCRIBE SCHEMA EXTENDED workshop_megacable.{usuario}_bronze"""

display(spark.sql(sql_command))

# COMMAND ----------

sql_command = f"""DESCRIBE SCHEMA EXTENDED workshop_megacable.{usuario}_silver"""

display(spark.sql(sql_command))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2.4 Cometarios sobre esquemas
# MAGIC
# MAGIC Utiliza el comando `ALTER SCHEMA <nombre_catalogo>.<nombre_esquema> SET COMMENT = 'comentario'` para describir el propósito del esquema. Añadir comentarios sobre los activos de datos aumenta la gobernanza y administración futura.

# COMMAND ----------

sql_command_bronze = f"""COMMENT ON SCHEMA workshop_megacable.{usuario}_bronze IS 'Esquema con información cruda (sin ninguna modificación) sobre la telcom ficticia Telio';"""
sql_command_silver = f"""COMMENT ON SCHEMA workshop_megacable.{usuario}_silver IS 'Esquema con información estructurada y limpia sobre la telcom ficticia Telio';"""
sql_command_gold = f"""COMMENT ON SCHEMA workshop_megacable.{usuario}_gold IS 'Esquema con información agregada sobre la telcom ficticia Telio';"""

display(spark.sql(sql_command_bronze))
display(spark.sql(sql_command_silver))
display(spark.sql(sql_command_gold))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Managed Tables
# MAGIC
# MAGIC Las "Managed Tables" en Unity Catalog son tablas gestionadas por Databricks, donde tanto los metadatos como los datos físicos (archivos en almacenamiento) son controlados automáticamente por la plataforma. Cuando creas una tabla gestionada, Unity Catalog se encarga de almacenar los archivos de datos en una ubicación definida por el sistema y administra toda la vida útil de esos datos, incluyendo su creación y eliminación.
# MAGIC
# MAGIC Para este laboratorio vamos a trabajar con las siguientes tablas de la telcom ficticia llamada Telio:
# MAGIC 1. **Clientes:** la tabla contiene información sobre clientes de una empresa de telecomunicaciones en California durante el segundo trimestre de 2024. Cada registro representa a un cliente y contiene detalles sobre sus datos demográficos, ubicación, antigüedad, servicios de suscripción, estado del trimestre (inscripción, permanencia o cancelación) y mucho más.
# MAGIC 2. **Código Postal**: Contiene información complementaria sobre las poblaciones estimadas para los códigos postales de California en la tabla de Clientes.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1 Crea una Managed Table
# MAGIC
# MAGIC 1. Utiliza el comando 
# MAGIC `CREATE OR REPLACE TABLE <nombre_catalogo>.<nombre_schema>.<nombre_tabla> ( { column_identifier column_type [ column_properties ] } [, ...]
# MAGIC     [ , table_constraint ] [...] )` para crear una tabla administrada en Unity Catalog
# MAGIC
# MAGIC     Toma en cuenta lo siguiente:
# MAGIC     * **column_identifier:** nombre de la columna
# MAGIC     * **column_type:** tipo de dato de la columna 
# MAGIC     * **column_properties:** propiedades adicionales de la columna como restricción a valores nulos, descripción, enmascaramiento, etc.
# MAGIC     * **table_constraint:** restricciones sobre la tabla como particiones, clusterización, descripción

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1.1 Creación de tablas base en la capa bronze

# COMMAND ----------

sql_command = f"""
CREATE OR REPLACE TABLE workshop_megacable.{usuario}_bronze.clientes (
  id_cliente STRING COMMENT 'ID único que identifica a cada CLIENTE',
  genero STRING COMMENT 'Género del cliente: masculino o femenino',
  edad STRING COMMENT 'Edad del cliente',
  casado STRING COMMENT 'Indica si el cliente está casado: TRUE, FALSE',
  dependientes STRING COMMENT 'Número de dependientes del cliente (hijos, padres, abuelos, etc)',
  ciudad STRING COMMENT 'Ciudad donde vive el cliente',
  codigo_postal STRING COMMENT 'Código postal de la ciudad donde vive el cliente',
  latitud STRING COMMENT 'Latitud de la casa asociada al cliente',
  longitud STRING COMMENT 'Longitud de la casa asociada al cliente',
  numero_referidos STRING COMMENT 'Indica el número de veces que el cliente a referido a un amigo o familiar al servicio de Telio',
  tenencia_meses STRING COMMENT 'Indica el número total de meses que el cliente ha tenido el servicio Telio',
  oferta STRING COMMENT 'Indica la última oferta de marketing que cliente aceptó',
  servicio_telefono STRING COMMENT 'Indica si el cliente se suscribió al servicio de teléfono',
  prom_mensual_larga_distancia STRING COMMENT 'Indica los cargos del cliente por llamada internacionales',
  multiples_lineas STRING COMMENT 'Indica si el cliente tiene más de una línea teléfonica',
  servicio_internet STRING COMMENT 'Indica si el cliente se suscribió al servicio de internet',
  tipo_internet STRING COMMENT 'Indica el tipo de servicio de internet que el cliente ha contratado',
  prom_mensual_gb_descarga STRING COMMENT 'Indica el promedio de GB de descarga mensual que el cliente ha consumido',
  seguridad_online STRING COMMENT 'Indicia si el cliente se suscribió al un plan adicional de seguridad',
  backup_online STRING COMMENT 'Indica si el cliente se suscribió al un plan adicional de backup',
  plan_proteccion_dispositivos STRING COMMENT 'Indica si el cliente se suscribió al un plan adicional de protección para su equipo de internet',
  soporte_premium STRING COMMENT 'Indica si el cliente se suscribió al un plan adicional de soporte premium con tiempos reducidos de atención',
  tv_streaming STRING,
  peliculas_streaming STRING COMMENT 'Indica si el cliente se suscribió al servicio de películas en streaming con un tercero sin costo adicional',
  musica_streaming STRING,
  datos_ilimitados STRING COMMENT 'Indica si el cliente se suscribió al servicio de datos ilimitados para descarga y subida',
  tipo_contrato STRING,
  recibo_electronico STRING COMMENT 'Indica si el cliente se suscribió al servicio de recibir el recibo de su factura por correo electrónico',
  tipo_pago STRING,
  cargo_mensual STRING COMMENT 'Indica el cargo mensual que el cliente debe pagar por todos sus servicios contratados',
  cargos_totales STRING COMMENT 'Indica los cargos totales pagados por el cliente al final del trimestre',
  total_reembolso STRING COMMENT 'Indica el total de reembolsos que el cliente ha recibido al final del trimestre',
  total_cargos_extra STRING,
  total_cargos_larga_distancia STRING,
  total_ingreso STRING COMMENT 'Indica el total de ingresos que el cliente ha generado para la compañía calclado al final del trimestre',
  estatus_cliente STRING,
  churn_cliente STRING,
  churn_razon STRING,
  _rescued_data STRING
);"""

display(spark.sql(sql_command))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1.2 Creación de tablas base en capa silver

# COMMAND ----------

sql_command = f"""CREATE OR REPLACE TABLE workshop_megacable.{usuario}_silver.clientes (
  id_cliente STRING COMMENT 'ID único que identifica a cada CLIENTE',
  genero STRING COMMENT 'Género del cliente: masculino o femenino',
  edad INT COMMENT 'Edad del cliente',
  casado STRING COMMENT 'Indica si el cliente está casado: TRUE, FALSE',
  dependientes INT COMMENT 'Número de dependientes del cliente (hijos, padres, abuelos, etc)',
  ciudad STRING COMMENT 'Ciudad donde vive el cliente',
  codigo_postal INT COMMENT 'Código postal de la ciudad donde vive el cliente',
  latitud DOUBLE COMMENT 'Latitud de la casa asociada al cliente',
  longitud DOUBLE COMMENT 'Longitud de la casa asociada al cliente',
  numero_referidos INT COMMENT 'Indica el número de veces que el cliente a referido a un amigo o familiar al servicio de Telio',
  tenencia_meses INT COMMENT 'Indica el número total de meses que el cliente ha tenido el servicio Telio',
  oferta STRING COMMENT 'Indica la última oferta de marketing que cliente aceptó',
  servicio_telefono STRING COMMENT 'Indica si el cliente se suscribió al servicio de teléfono',
  prom_mensual_larga_distancia DOUBLE COMMENT 'Indica los cargos del cliente por llamada internacionales',
  multiples_lineas STRING COMMENT 'Indica si el cliente tiene más de una línea teléfonica',
  servicio_internet STRING COMMENT 'Indica si el cliente se suscribió al servicio de internet',
  tipo_internet STRING COMMENT 'Indica el tipo de servicio de internet que el cliente ha contratado',
  prom_mensual_gb_descarga DOUBLE COMMENT 'Indica el promedio de GB de descarga mensual que el cliente ha consumido',
  seguridad_online STRING COMMENT 'Indicia si el cliente se suscribió al un plan adicional de seguridad',
  backup_online STRING COMMENT 'Indica si el cliente se suscribió al un plan adicional de backup',
  plan_proteccion_dispositivos STRING COMMENT 'Indica si el cliente se suscribió al un plan adicional de protección para su equipo de internet',
  soporte_premium STRING COMMENT 'Indica si el cliente se suscribió al un plan adicional de soporte premium con tiempos reducidos de atención',
  tv_streaming STRING,
  peliculas_streaming STRING COMMENT 'Indica si el cliente se suscribió al servicio de películas en streaming con un tercero sin costo adicional',
  musica_streaming STRING,
  datos_ilimitados STRING COMMENT 'Indica si el cliente se suscribió al servicio de datos ilimitados para descarga y subida',
  tipo_contrato STRING,
  recibo_electronico STRING COMMENT 'Indica si el cliente se suscribió al servicio de recibir el recibo de su factura por correo electrónico',
  tipo_pago STRING,
  cargo_mensual DOUBLE COMMENT 'Indica el cargo mensual que el cliente debe pagar por todos sus servicios contratados',
  cargos_totales DOUBLE COMMENT 'Indica los cargos totales pagados por el cliente al final del trimestre',
  total_reembolso DOUBLE COMMENT 'Indica el total de reembolsos que el cliente ha recibido al final del trimestre',
  total_cargos_extra DOUBLE,
  total_cargos_larga_distancia DOUBLE,
  total_ingreso DOUBLE COMMENT 'Indica el total de ingresos que el cliente ha generado para la compañía calclado al final del trimestre',
  estatus_cliente STRING,
  churn_cliente STRING,
  churn_razon STRING,
  id_codigo_postal INTEGER,
  poblacion INTEGER
);"""

display(spark.sql(sql_command))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1.3 Creación de tablas base en capa gold

# COMMAND ----------

sql_command = f"""CREATE OR REPLACE TABLE workshop_megacable.{usuario}_gold.clientes (
  id_cliente STRING COMMENT 'ID único que identifica a cada CLIENTE',
  genero STRING COMMENT 'Género del cliente: masculino o femenino',
  edad INT COMMENT 'Edad del cliente',
  casado STRING COMMENT 'Indica si el cliente está casado: TRUE, FALSE',
  dependientes INT COMMENT 'Número de dependientes del cliente (hijos, padres, abuelos, etc)',
  ciudad STRING COMMENT 'Ciudad donde vive el cliente',
  codigo_postal INT COMMENT 'Código postal de la ciudad donde vive el cliente',
  latitud DOUBLE COMMENT 'Latitud de la casa asociada al cliente',
  longitud DOUBLE COMMENT 'Longitud de la casa asociada al cliente',
  numero_referidos INT COMMENT 'Indica el número de veces que el cliente a referido a un amigo o familiar al servicio de Telio',
  tenencia_meses INT COMMENT 'Indica el número total de meses que el cliente ha tenido el servicio Telio',
  oferta STRING COMMENT 'Indica la última oferta de marketing que cliente aceptó',
  servicio_telefono STRING COMMENT 'Indica si el cliente se suscribió al servicio de teléfono',
  prom_mensual_larga_distancia DOUBLE COMMENT 'Indica los cargos del cliente por llamada internacionales',
  multiples_lineas STRING COMMENT 'Indica si el cliente tiene más de una línea teléfonica',
  servicio_internet STRING COMMENT 'Indica si el cliente se suscribió al servicio de internet',
  tipo_internet STRING COMMENT 'Indica el tipo de servicio de internet que el cliente ha contratado',
  prom_mensual_gb_descarga DOUBLE COMMENT 'Indica el promedio de GB de descarga mensual que el cliente ha consumido',
  seguridad_online STRING COMMENT 'Indicia si el cliente se suscribió al un plan adicional de seguridad',
  backup_online STRING COMMENT 'Indica si el cliente se suscribió al un plan adicional de backup',
  plan_proteccion_dispositivos STRING COMMENT 'Indica si el cliente se suscribió al un plan adicional de protección para su equipo de internet',
  soporte_premium STRING COMMENT 'Indica si el cliente se suscribió al un plan adicional de soporte premium con tiempos reducidos de atención',
  tv_streaming STRING,
  peliculas_streaming STRING COMMENT 'Indica si el cliente se suscribió al servicio de películas en streaming con un tercero sin costo adicional',
  musica_streaming STRING,
  datos_ilimitados STRING COMMENT 'Indica si el cliente se suscribió al servicio de datos ilimitados para descarga y subida',
  tipo_contrato STRING,
  recibo_electronico STRING COMMENT 'Indica si el cliente se suscribió al servicio de recibir el recibo de su factura por correo electrónico',
  tipo_pago STRING,
  cargo_mensual DOUBLE COMMENT 'Indica el cargo mensual que el cliente debe pagar por todos sus servicios contratados',
  cargos_totales DOUBLE COMMENT 'Indica los cargos totales pagados por el cliente al final del trimestre',
  total_reembolso DOUBLE COMMENT 'Indica el total de reembolsos que el cliente ha recibido al final del trimestre',
  total_cargos_extra DOUBLE,
  total_cargos_larga_distancia DOUBLE,
  total_ingreso DOUBLE COMMENT 'Indica el total de ingresos que el cliente ha generado para la compañía calclado al final del trimestre',
  estatus_cliente STRING,
  churn_cliente STRING,
  churn_razon STRING,
  id_codigo_postal INTEGER,
  poblacion INTEGER
);"""

display(spark.sql(sql_command))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Utiliza Catalog Explorer
# MAGIC
# MAGIC Catalog Explorer en Unity Catalog es una interfaz gráfica (UI) que permite explorar y gestionar de manera centralizada datos, esquemas (bases de datos), tablas, modelos, funciones y otros activos de inteligencia artificial en tu entorno de Databricks.
# MAGIC
# MAGIC Puedes acceder a Catalog Explorar desde el Menú Izquierdo de Datarbricks
# MAGIC <br> </br>
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="images/Catalog_Explorer.png" alt="Descripción" width="800">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.4.1 Busca objetos dentro de Catalog Explorer
# MAGIC
# MAGIC Puedes utilizar la barra de búsqueda de Catalog Exlorer para buscar catálogos, esquemas, tablas, funciones, modelos ML, todo lo que sea trazeable por Unity Catalog.
# MAGIC
# MAGIC Busca el catálogo de este laboratorio: **workshop_megacable**
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="images/Barra_Busqueda_Catalog_Explorer.png" alt="Descripción" width="800">
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.4.2 Utiliza Catalog Explorer para crear Objetos
# MAGIC
# MAGIC Crear esquemas
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="images/Crear_Esquemas.png" alt="Descripción" width="800">
# MAGIC </div>
# MAGIC
# MAGIC <br></br>
# MAGIC Dentro de los esquemas puedes crear tablas, volúmenes, modelos, vistas de métricas
# MAGIC <div align="center">
# MAGIC   <img src="images/Crear_Objetos_en_Esquema.png" alt="Descripción" width="800">
# MAGIC </div>
# MAGIC
# MAGIC <br></br>
# MAGIC Cada tabla la puedes asociar con un componente diferente de Databricks... 
# MAGIC <div align="center">
# MAGIC   <img src="images/Tabla_Assets.png" alt="Descripción" width="800">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.4.3 Utiliza AI Assistant en Unity Catalog
# MAGIC
# MAGIC El AI Assistant de Unity Catalog es una funcionalidad integrada en Databricks que utiliza inteligencia artificial generativa para sugerir automáticamente descripciones y comentarios en objetos como catálogos, esquemas, tablas, columnas, volúmenes y modelos dentro de Catalog Explorer. Su propósito principal es simplificar y acelerar la documentación, curación y descubrimiento de los activos de datos y modelos de una organización, resolviendo el frecuente problema de falta o baja calidad en las descripciones manuales.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Puedes utilizar el AI Assistant para generar descripciones de tablas, solo de click en **Accept** o **Edit**, según sea el caso 
# MAGIC <div align="center">
# MAGIC   <img src="images/AI_Assistant_Tabla.png" alt="Descripción" width="800">
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC También puedes utilizar AI Assistant para añadir comentarios sobres las columnas.
# MAGIC 1. Primero click sobre **Añadir comentario**
# MAGIC <div align="center">
# MAGIC   <img src="images/AI_Assistant_Columna1.png" alt="Descripción" width="800">
# MAGIC </div>
# MAGIC
# MAGIC 2. Luego puedes utilizar AI Assistant
# MAGIC <div align="center">
# MAGIC   <img src="images/AI_Assistant_Columna2.png" alt="Descripción" width="800">
# MAGIC </div>
# MAGIC
# MAGIC 3. Puedes seguir utilizando AI Assistant para traducir o acortar el texto
# MAGIC <div align="center">
# MAGIC   <img src="images/AI_Assistant_Columna3.png" alt="Descripción" width="800">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.4.4 Explora más sobre Unity Catalog
# MAGIC
# MAGIC Utiliza los Product Tours y Tutoriales de Databricks para explorar más sobre Unity Catalog, haz click en el siguiente enlace:
# MAGIC
# MAGIC - Product Tours: https://www.databricks.com/resources/demos/library?type=2030&platform=2520
# MAGIC - Tutoriales: https://www.databricks.com/resources/demos/library?type=2031&platform=2520

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tags
# MAGIC
# MAGIC Las etiquetas (tags) en Databricks son atributos que consisten en claves y valores opcionales, utilizados para organizar y categorizar objetos en la plataforma. Estas etiquetas permiten facilitar la búsqueda y el descubrimiento de componentes; además de garantizar un gestión correcta de costos dentro de la plataforma.
# MAGIC
# MAGIC <!-- <div align="center">
# MAGIC   <img src="images/Cost_Tags.png" alt="Descripción" width="600">
# MAGIC </div> -->

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Define tags sobre las tablas creadas
# MAGIC Tags sobre la tabla Clientes en la capa Bronze

# COMMAND ----------

sql_command = f"""ALTER TABLE workshop_megacable.{usuario}_bronze.clientes
SET TAGS ('t_project' = 'workshop_megacable');"""

display(spark.sql(sql_command))

# COMMAND ----------

# MAGIC %md
# MAGIC Tags sobre la tabla Clientes en la capa Silver

# COMMAND ----------

sql_command = f"""ALTER TABLE workshop_megacable.{usuario}_silver.clientes
SET TAGS ('t_project' = 'workshop_megacable');"""

display(spark.sql(sql_command))

# COMMAND ----------

# MAGIC %md
# MAGIC Tags sobre la tabla Clientes en la capa Gold

# COMMAND ----------

sql_command = f"""ALTER TABLE workshop_megacable.{usuario}_gold.clientes
SET TAGS ('t_project' = 'workshop_megacable');"""

display(spark.sql(sql_command))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Visualiza los tags creados en las tablas

# COMMAND ----------

# MAGIC %md
# MAGIC Ingresa a la tabla deseada a través de Catalog Explorer y en el siguiente apartado puedes visualizar las etiquteas creadas
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="images/Tags.png" alt="Descripción" width="800">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. BONUS: Control de Permisos en Unity Catalog
# MAGIC
# MAGIC El control de accesos en Unity Catalog es el conjunto de mecanismos y políticas que permiten gestionar, de manera centralizada, quién y cómo puede ver, consultar, modificar o administrar los diferentes recursos de datos (como catálogos, esquemas, tablas, vistas, volúmenes, modelos de ML) dentro de la plataforma Databricks.
# MAGIC
# MAGIC Este control funciona a través de la asignación de permisos específicos (privilegios) a "principales" (usuarios, grupos y service principals) sobre objetos jerárquicos (catálogos, esquemas, tablas, etc.), evitando accesos indebidos y asegurando la gobernanza, trazabilidad y cumplimiento normativo.
# MAGIC
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="images/Permisos_Unity_Catalog.png" alt="Descripción" width="800">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1. Creación de Tabla Dummy
# MAGIC
# MAGIC Vamos a crear una tabla dummy para ver la asignación de permisos a nivel fila y columna

# COMMAND ----------

from pyspark.sql import Row
import random

# Crear esquema dummy
spark.sql(f"""CREATE SCHEMA IF NOT EXISTS workshop_megacable.{usuario}_dummy""")

# Listas de valores dummy
regiones = ["Norte", "Sur", "Este", "Oeste"]
paquetes = ["Básico", "Premium", "VIP"]
# Generar datos dummy
data = [
    Row(
        id=i+1,
        region=random.choice(regiones),
        paquete=random.choice(paquetes)
    )
    for i in range(100)
]
# Crear DataFrame
df = spark.createDataFrame(data)
# Guardar en Unity Catalog
# Formato: <catalogo>.<esquema>.<nombre_tabla>
catalog_name = "workshop_megacable"
schema_name = f"{usuario}_dummy"
table_name = "tabla_dummy"
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.1.1 Consulta la tabla creada recientemente

# COMMAND ----------

display(spark.read.table(f"{catalog_name}.{schema_name}.{table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Define una función a nivel filtro-fila
# MAGIC
# MAGIC Row-Level Security (RLS) en Unity Catalog es un mecanismo que permite controlar, de manera granular, el acceso a las filas específicas dentro de una tabla según atributos del usuario, grupo o contexto de consulta. Esto significa que diferentes usuarios o grupos pueden ver únicamente los registros (filas) a los que están autorizados, aun cuando consulten la misma tabla, alineando el acceso a datos con políticas organizacionales de seguridad y cumplimiento.

# COMMAND ----------

sql_command = f"""
CREATE OR REPLACE FUNCTION workshop_megacable.{usuario}_dummy.filtro_usuario(region STRING)
RETURN IF(
  CURRENT_USER() = '{email}',
  region = 'Sur',               -- Solo ve filas donde region es 'MX'
  FALSE                        -- Otros usuarios no ven ninguna fila (puede cambiarse según el caso)
);
"""

display(spark.sql(sql_command))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.2.1 Aplica la función de filtro-fila
# MAGIC Vamos a aplicar la función **filtro_usuario** sobre la **tabla_dummy** recientemente creada

# COMMAND ----------

sql_command = f"""ALTER TABLE workshop_megacable.{usuario}_dummy.tabla_dummy
SET ROW FILTER workshop_megacable.{usuario}_dummy.filtro_usuario ON (region);"""

display(spark.sql(sql_command))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.2.2 Consulta la tabla dummy
# MAGIC Consulta nuevamente la tabla dummy, esta vez obtendrás las filas donde la región sea 'Sur'.

# COMMAND ----------

display(spark.read.table(f"{catalog_name}.{schema_name}.{table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Define una función de enmascaramiento a nivel columna
# MAGIC
# MAGIC Column-Level Masking en Unity Catalog es una funcionalidad de seguridad que permite ocultar, transformar o reemplazar dinámicamente el contenido de columnas sensibles en tablas, de acuerdo con las políticas de acceso definidas para cada usuario o grupo. 
# MAGIC
# MAGIC La máscara de columna se evalúa en tiempo de consulta, lo que significa que el dato original permanece sin alterar en el almacenamiento, pero los resultados presentados al usuario varían según sus privilegios.

# COMMAND ----------

sql_command = f"""CREATE OR REPLACE FUNCTION workshop_megacable.{usuario}_dummy.mascara_ssn(ssn STRING) 
RETURN IF(current_user() = '{email}', '*****', ssn);""" 

display(spark.sql(sql_command))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.3.1 Aplica la función de enmascaramiento

# COMMAND ----------

sql_command = f"""ALTER TABLE workshop_megacable.{usuario}_dummy.tabla_dummy ALTER COLUMN paquete SET MASK workshop_megacable.{usuario}_dummy.mascara_ssn;""" 

display(spark.sql(sql_command))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.3.2 Consulta la tabla dummy
# MAGIC Consulta nuevamente la tabla dummy, esta vez obtendrás la columna paquete con el enmascaramiento

# COMMAND ----------

display(spark.read.table(f"{catalog_name}.{schema_name}.{table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Recap
# MAGIC Hemos llegado al final de nuestro primer notebook sobre Gobernanza de Datos. Repasemos los conceptos clave que cubrimos:
# MAGIC
# MAGIC 1. **Desafíos en la gobernanza:** Vimos que la gobernanza de datos es difícil debido a la fragmentación, la falta de inteligencia en los datos y la mala conectividad entre plataformas.
# MAGIC
# MAGIC 2. **La solución de Databricks:** Descubrimos cómo Databricks resuelve estos problemas con su enfoque abierto, gobernando activos de datos estructurados y no estructurados, y centralizando la gestión de accesos.
# MAGIC
# MAGIC 3. **Unity Catalog en acción:** Usamos código y la interfaz de usuario para crear objetos como esquemas y tablas dentro de Unity Catalog.
# MAGIC
# MAGIC 4. **Gestión y costos:** Aprendimos a definir etiquetas (tags), una herramienta fundamental para la gobernanza y para una buena gestión de costos en la plataforma.
# MAGIC
# MAGIC 5. **Control de acceso:** Definimos funciones para aplicar control de acceso a nivel de fila y columna, lo que nos da un control granular sobre quién puede ver qué datos.

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
# MAGIC Haz acabado la  primera parte de este laborario práctico, ahora sabes que mediante Unity Catalog de Databricks puedes realizar gobernanza de datos de una forma fácil y práctica.
# MAGIC
# MAGIC La siguiente parte del laborario se centra en Data Engineering. Vamos a ello:
# MAGIC
# MAGIC [Data Governance]($./01. Data Governance)
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="images/Cat_Im_Cooking.gif" alt="Descripción" width="400">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ____

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
