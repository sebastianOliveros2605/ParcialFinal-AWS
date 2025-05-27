# glue_jobs/download_headlines.py

import boto3
import requests
from datetime import date

BUCKET = 'guardar-html'
SITES = {
    'eltiempo': 'https://www.eltiempo.com/',
    'elespectador': 'https://www.elespectador.com/'
}

def download_page(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        raise Exception(f"Error al descargar {url}: {response.status_code}")

def generate_s3_key(site_name, fecha=None):
    if fecha is None:
        fecha = date.today().isoformat()
    return f"headlines/raw/{site_name}-contenido-{fecha}.html"

def save_to_s3(bucket, key, content):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=content)

def main():
    for name, url in SITES.items():
        content = download_page(url)
        key = generate_s3_key(name)
        save_to_s3(BUCKET, key, content)

if __name__ == '__main__':
    main()
