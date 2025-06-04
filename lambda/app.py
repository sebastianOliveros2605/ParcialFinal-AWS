import boto3
import json
import time
from datetime import datetime
from botocore.exceptions import ClientError

emr_client = boto3.client('emr', region_name='us-east-1')

BASE_CLUSTER_ID = 'j-VHXOM6L67C7O'
RELEASE_LABEL = 'emr-6.15.0'
STEP_SCRIPT_S3_PATH = 's3://resulta2-emr/scripts/script_modelo.py'  # ACTUALIZA ESTO
OUTPUT_S3_PATH = 's3://resulta2-emr/resultados-modelo/'  # ACTUALIZA ESTO

def launch_emr_job(event, context=None):
    try:
        print(f"Clonando configuración del clúster base: {BASE_CLUSTER_ID}")
        response = emr_client.describe_cluster(ClusterId=BASE_CLUSTER_ID)
        base_config = response['Cluster']

        applications = [{'Name': app['Name']} for app in base_config['Applications']]
        print("Aplicaciones sin versiones:", applications)

        cluster_name = f"ClonedNewsCluster-{int(time.time())}"

        cluster_response = emr_client.run_job_flow(
            Name=cluster_name,
            ReleaseLabel=RELEASE_LABEL,
            Applications=applications,
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1
                    },
                    {
                        'Name': 'Core nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 2
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': False,  # <- Apaga el cluster cuando termine
                'TerminationProtected': False
            },
            BootstrapActions=[],
            Steps=[
                {
                    'Name': 'Run Spark headline classifier',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-submit',
                            '--deploy-mode', 'cluster',
                            STEP_SCRIPT_S3_PATH,
                            '--output', OUTPUT_S3_PATH
                        ]
                    }
                }
            ],
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True,
            LogUri='s3://your-log-bucket/emr-logs/'  # <- Cambia esto si lo necesitas
        )

        cluster_id = cluster_response['JobFlowId']
        print(f"Nuevo clúster EMR lanzado con ID: {cluster_id}")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'EMR cluster launched and step added successfully',
                'cluster_id': cluster_id
            })
        }

    except ClientError as e:
        print(f"Error al lanzar el clúster EMR: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Error launching EMR cluster: {e}'})
        }
