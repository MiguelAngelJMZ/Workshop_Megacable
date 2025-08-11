# Databricks notebook source
# MAGIC %pip install --upgrade databricks-sdk -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines
import time

# COMMAND ----------

w = WorkspaceClient()
email = w.current_user.me().user_name
usuario = email.split("@")[0].replace(".", "_").replace("+", "_").replace("-", "_")

# COMMAND ----------

input_path = f"/Volumes/workshop_megacable/{usuario}_raw_data/clientes/raw_data/"
checkpoint_path = f"/Volumes/workshop_megacable/{usuario}_raw_data/clientes/checkpoints/"
table_name = f"workshop_megacable.{usuario}_bronze.clientes"  
schema_location = f"/Volumes/workshop_megacable/{usuario}_raw_data/clientes/schemas/"

# COMMAND ----------

print(f"""Usuario: {usuario} 
Email: {email}""")
