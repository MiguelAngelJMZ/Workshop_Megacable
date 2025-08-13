# Databricks notebook source
# MAGIC %pip install --upgrade databricks-sdk -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines
import time

w = WorkspaceClient()
email = w.current_user.me().user_name
usuario = email.split("@")[0].replace(".", "_")

notebook_path = f'/Workspace/Users/{email}/Workshop_Megacable/setup/00_Lakeflow_Declarative_Pipeline/Codigo_Postal'


name = f"{usuario}_pipeline_codigo_postal"
schema = f"{usuario}_bronze"

# COMMAND ----------

try:
    created = w.pipelines.create(
        continuous=False,
        name=name,
        catalog="workshop_megacable",
        schema=schema,
        libraries=[pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=notebook_path))],
        serverless=True
    )
    pipeline_url = f"{w.config.host}/pipelines/{created.pipeline_id}"

    print("Accede a la UI del pipeline en:", pipeline_url)
except Exception as e:
    print(e)
