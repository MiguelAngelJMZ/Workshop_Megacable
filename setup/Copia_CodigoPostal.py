# Databricks notebook source
# MAGIC %pip install databricks-sdk -q
# MAGIC
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service import pipelines
# MAGIC import time

# COMMAND ----------

w = WorkspaceClient()
email = w.current_user.me().user_name
usuario = email.split("@")[0].replace(".", "_").replace("+", "_").replace("-", "_")

# COMMAND ----------

spark.sql(f"""CREATE SCHEMA IF NOT EXISTS workshop_megacable.{usuario}_raw_data""")

# COMMAND ----------

spark.sql(f"""CREATE VOLUME IF NOT EXISTS workshop_megacable.{usuario}_raw_data.codigo_postal""")


# COMMAND ----------

download_url = "https://raw.githubusercontent.com/MiguelAngelJMZ/Data_WorkshopMegacable/refs/heads/main/codigo_postal.csv"
file_name = "codigo_postal.csv"
catalog = 'workshop_megacable'
schema = usuario + "_raw_data"
volume = "codigo_postal/raw_data"
path_volume = "/Volumes/" + catalog + "/" + schema + "/" + volume
dbutils.fs.cp(f"{download_url}", f"{path_volume}" + "/" + f"{file_name}")


# COMMAND ----------

df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(path_volume + "/" + file_name)

# COMMAND ----------

display(df.limit(10))