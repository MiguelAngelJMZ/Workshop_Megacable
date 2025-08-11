# Databricks notebook source
# MAGIC %pip install databricks-sdk -q
# MAGIC
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service import pipelines
# MAGIC import time
# MAGIC
# MAGIC w = WorkspaceClient()

# COMMAND ----------

w = WorkspaceClient()
email = w.current_user.me().user_name
usuario = usuario.split("@")[0].replace(".", "_")

# COMMAND ----------

w.jobs.list()

# COMMAND ----------

notebook_path = f'/Workspace/Users/{email}/Pruebas_Workshop/setup/00_Lakeflow_Declarative_Pipeline/Codigo_Postal'

name = f"{usuario}_pipeline_codigo_postal"

try:
    created = w.pipelines.create(
        continuous=False,
        name=name,
        catalog='workshop_megacable',
        schema=f'{usuario}_bronze',
        libraries=[pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=notebook_path))],
        serverless=True,
        allow_duplicate_names=True
    )
    pipeline_url = f"{w.config.host}/pipelines/{created.pipeline_id}"

    print("Accede a la UI del pipeline en:", pipeline_url)
except Exception as e:
    print(e)