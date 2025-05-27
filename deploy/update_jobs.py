import boto3

glue = boto3.client('glue')

jobs = {
    'download_headlines': 's3://guardar-html/scripts-glue/download_headlines.py',
    'parse_headlines': 's3://guardar-html/scripts-glue/parse_headlines.py'
}

for job_name, s3_path in jobs.items():
    response = glue.update_job(
        JobName=job_name,
        JobUpdate={
            'Command': {
                'Name': 'glueetl',
                'ScriptLocation': s3_path,
                'PythonVersion': '3'
            }
        }
    )
    print(f"Job '{job_name}' actualizado con script: {s3_path}")
