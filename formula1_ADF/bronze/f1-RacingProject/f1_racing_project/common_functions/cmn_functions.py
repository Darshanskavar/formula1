# Databricks notebook source
def create_mount_point(container_name,storage_acc_name):
    """
    This function will create mount point for azure storage account

    """
    client_id = dbutils.secrets.get('sample_project_scope','access-client-ID')
    tenant_id = dbutils.secrets.get('sample_project_scope','access-tenant-ID')
    client_secretes = dbutils.secrets.get('sample_project_scope','access-client-secrets')

    configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": client_id,
       "fs.azure.account.oauth2.client.secret": client_secretes,
       "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

    l= []
    for mount in dbutils.fs.mounts():
        if mount.mountPoint == f'/mnt/{storage_acc_name}/{container_name}':
            l.append(mount.mountPoint)
        else:
            pass
    if any(l):
        dbutils.fs.unmount(f'/mnt/{storage_acc_name}/{container_name}')
        dbutils.fs.mount(source=f'abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net',mount_point=f'/mnt/{storage_acc_name}/{container_name}',extra_configs=configs)
    else:
        dbutils.fs.mount(source=f'abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net',mount_point=f'/mnt/{storage_acc_name}/{container_name}',extra_configs=configs)
                 

    



# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,DateType
from pyspark.sql.functions import current_date,col,concat,lit,regexp_replace,when,max,window
from datetime import datetime,date

# COMMAND ----------

def add_column(output_df):
    output_df = output_df.withColumn("Ingestion_date",current_date())
    return output_df
