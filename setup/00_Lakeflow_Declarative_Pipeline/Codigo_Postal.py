# Databricks notebook source
# %pip install databricks-sdk

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
email = w.current_user.me().user_name
usuario = email.split("@")[0].replace(".", "_")

# COMMAND ----------

import dlt
from pyspark.sql.functions import col



# Configura la ruta origen del volumen (ajusta según tu entorno)
VOL_PATH = f"/Volumes/workshop_megacable/{usuario}_raw_data/codigo_postal/raw_data/"

# 1. Bronze: ingesta desde volumen
@dlt.table(
    comment="Tabla bronze: datos crudos desde el volumen",
    name=f"workshop_megacable.{usuario}_bronze.codigo_postal")
def bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")  # ajusta el formato (csv, json, parquet, etc.)
        .load(VOL_PATH)
    )

# 2. Silver: aplica filtro para eliminar registros cuyo id empieza con '5'
@dlt.table(
    comment="Tabla silver: datos filtrados; eliminados ids que inician con 5",
    name=f"workshop_megacable.{usuario}_silver.codigo_postal")
def silver():
    return (
        dlt.read(f"workshop_megacable.{usuario}_bronze.codigo_postal")
        .filter(~col("id_codigo_postal").cast("string").startswith("5")).withColumn("id_codigo_postal", col("id_codigo_postal").cast("integer")).withColumn("poblacion", col("poblacion").cast("integer"))
    )

# 3. Gold: aplica transformaciones adicionales
@dlt.table(
    comment="Tabla gold: datos transformados",
    name=f"workshop_megacable.{usuario}_gold.codigo_postal")
def gold():
    return (
        dlt.read(f"workshop_megacable.{usuario}_silver.codigo_postal"))
