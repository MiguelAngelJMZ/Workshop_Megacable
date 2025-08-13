# Databricks notebook source
# MAGIC %md
# MAGIC ![Problemas Gobernanza de Datos](images/Databricks_Logo.png)
# MAGIC
# MAGIC
# MAGIC Presentan:
# MAGIC
# MAGIC * Miguel Jiménez - Sr. Solutions Engineer
# MAGIC * Alberto Ramírez - Solutions Architect
# MAGIC * Nicky Moreno - Business Development Representative
# MAGIC * Alfredo Rubio - Sales Hispanic LATAM Director

# COMMAND ----------

# MAGIC %md
# MAGIC ## Laboratorio Databricks: Fundamentos y Herramientas Clave en Lakehouse
# MAGIC
# MAGIC
# MAGIC ¡Prepárate para dominar Databricks! En este laboratorio, te sumergirás en los fundamentos de la plataforma, aprendiendo a potenciar tus habilidades en Data Engineering y Business Intelligence.
# MAGIC
# MAGIC A lo largo de la sesión, descubrirás los pilares clave que hacen de Databricks una herramienta líder en la industria:
# MAGIC
# MAGIC 1. Arquitectura Lakehouse: Conoce el modelo que combina lo mejor de los Data Lakes y los Data Warehouses.
# MAGIC
# MAGIC 2. Data Governance con Unity Catalog: Aprende a gestionar y asegurar tus datos de manera centralizada.
# MAGIC
# MAGIC 3. Data Engineering con Auto Loader y Pipelines: Automatiza tus procesos de datos con eficiencia y fiabilidad.
# MAGIC
# MAGIC 4. Business Intelligence con AI/BI Dashboards: Transforma tus datos en visualizaciones dinámicas y obtén insights valiosos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Agenda
# MAGIC
# MAGIC Para garantizar un aprendizaje práctico y efectivo, contarás con una serie de notebooks autodescriptivos. Sigue la secuencia de ejecución de cada uno y, paso a paso, dominarás los conceptos y la implementación del código. 
# MAGIC <br>
# MAGIC
# MAGIC | # | Notebook |
# MAGIC | --- | --- |
# MAGIC | 01 | [Data Governance]($./01. Data Governance) | 
# MAGIC | 02 | [Data Engineering]($./02. Data Engineering) |
# MAGIC | 03 | [Data Visualization]($./03. Data Visualizaciones) |
# MAGIC
# MAGIC <br>
# MAGIC Al finalizar, habrás adquirido una base sólida en Databricks, con conocimientos prácticos y aplicables a proyectos reales de Data Engineering y Business Intelligence.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Datos
# MAGIC Para este laboratorio vamos a ocupar el dataset de un empresa telecom ficticia llamada Telio. Este dataset consiste en dos tablas:
# MAGIC
# MAGIC 1. **Clientes:** la tabla de abandono de clientes contiene información sobre los 7043 clientes de una empresa de telecomunicaciones llamada Telio, en California, durante el segundo trimestre de 2022. Cada registro representa a un cliente y contiene detalles sobre sus datos demográficos, ubicación, antigüedad, servicios de suscripción, estado del trimestre (incorporación, permanencia o abandono) y mucho más.
# MAGIC 2. **Código Postal:** la tabla de código postal contiene información complementaria sobre las poblaciones estimadas para los códigos postales de California en la tabla de clientes.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Prerrequisitos
# MAGIC El contenido fue desarrollado tomando en cuenta que los participantes tienen los siguientes privilegios:
# MAGIC - USE CATALOG workshop_megacable
# MAGIC - Uso de Compute Serverless

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
