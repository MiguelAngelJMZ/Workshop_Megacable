# Databricks notebook source
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines
import time

# COMMAND ----------

w = WorkspaceClient()
email = w.current_user.me().user_name
usuario = email.split("@")[0].replace(".", "_").replace("+", "_").replace("-", "_")

# COMMAND ----------

spark.sql(f"""CREATE SCHEMA IF NOT EXISTS workshop_megacable.{usuario}_raw_data""")

# COMMAND ----------

spark.sql(f"""CREATE VOLUME IF NOT EXISTS workshop_megacable.{usuario}_raw_data.clientes""")


# COMMAND ----------

input_path = f"/Volumes/workshop_megacable/{usuario}_raw_data/clientes/raw_data/"
checkpoint_path = f"/Volumes/workshop_megacable/{usuario}_raw_data/clientes/checkpoints/"
table_name = f"workshop_megacable.{usuario}_bronze.clientes"  
schema_location = f"/Volumes/workshop_megacable/{usuario}_raw_data/clientes/schemas/"

# COMMAND ----------

download_url = "https://raw.githubusercontent.com/MiguelAngelJMZ/Data_WorkshopMegacable/refs/heads/main/clientes_parte1.csv"
file_name = "clientes_parte1.csv"
catalog = 'workshop_megacable'
schema = usuario + "_raw_data"
volume = "clientes/raw_data"
path_volume = "/Volumes/" + catalog + "/" + schema + "/" + volume
dbutils.fs.cp(f"{download_url}", f"{path_volume}" + "/" + f"{file_name}")


# COMMAND ----------

df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(path_volume + "/" + file_name)

# COMMAND ----------

display(df.limit(10))
