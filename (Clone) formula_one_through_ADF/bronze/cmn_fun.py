# Databricks notebook source
def create_mount_point(container_name,storage_acc_name):
    """
    This function will create mount point for azure storage account

    """
    client_id =dbutils.secrets.get(scope='scope-formula1',key='client-id-app')
    tenant_id =dbutils.secrets.get(scope='scope-formula1',key='tenant-id-dir')
    client_secret_value =dbutils.secrets.get(scope='scope-formula1',key='secret-value-client')

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
                 

    


