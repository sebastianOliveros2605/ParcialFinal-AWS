import boto3

glue = boto3.client('glue')

# Parámetros compartidos
role_arn = 'arn:aws:iam::302119430275:role/LabRole'
s3_bucket_scripts = 's3://guardar-html/scripts-glue/' # Definir el bucket base para los scripts

# Actualizar download_headlines a Python Shell
try:
    glue.update_job(
        JobName='download_headlines',
        JobUpdate={
            'Role': role_arn,
            'Command': {
                'Name': 'pythonshell', # <--- CAMBIO CLAVE
                'ScriptLocation': f'{s3_bucket_scripts}download_headlines.py',
                'PythonVersion': '3'   # Especifica la versión de Python
            },

        }
    )
    print("Job 'download_headlines' actualizado a Python Shell exitosamente.")
except Exception as e:
    print(f"Error actualizando 'download_headlines': {e}")

# Actualizar parse_headlines a Python Shell
try:
    glue.update_job(
        JobName='parse_headlines',
        JobUpdate={
            'Role': role_arn,
            'Command': {
                'Name': 'pythonshell', # <--- CAMBIO CLAVE
                'ScriptLocation': f'{s3_bucket_scripts}parse_headlines.py',
                'PythonVersion': '3'   # Especifica la versión de Python
            },
            # Al igual que arriba, tener cuidado con parámetros residuales de Spark.
        }
    )
    print("Job 'parse_headlines' actualizado a Python Shell exitosamente.")
except Exception as e:
    print(f"Error actualizando 'parse_headlines': {e}")