import boto3

glue = boto3.client('glue')

# Parámetros compartidos
role_arn = 'arn:aws:iam::302119430275:role/LabRole'  # <-- usa el ARN real de tu rol

# Actualizar download_headlines
glue.update_job(
    JobName='download_headlines',
    JobUpdate={
        'Role': role_arn,
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': 's3://guardar-html/scripts-glue/download_headlines.py',
            'PythonVersion': '3'
        }
    }
)

# Actualizar parse_headlines
glue.update_job(
    JobName='parse_headlines',
    JobUpdate={
        'Role': role_arn,
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': 's3://guardar-html/scripts-glue/parse_headlines.py',
            'PythonVersion': '3'
        }
    }
)
