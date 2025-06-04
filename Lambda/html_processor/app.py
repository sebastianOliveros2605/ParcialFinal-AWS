import boto3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import unquote_plus

s3 = boto3.client('s3')

def handler(event, context):
    # Procesar cada archivo recibido
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        if not key.endswith('.html'):
            continue

        # Detectar periódico
        if 'eltiempo' in key:
            periodico = 'eltiempo'
        elif 'elespectador' in key:
            periodico = 'elespectador'
        else:
            periodico = 'desconocido'

        # Obtener la fecha del nombre del archivo
        date_part = key.split('/')[-1].replace('.html', '').split('-')[-3:]
        yyyy, mm, dd = date_part

        # Descargar el archivo
        response = s3.get_object(Bucket=bucket, Key=key)
        html = response['Body'].read().decode('utf-8')

        soup = BeautifulSoup(html, 'html.parser')
        articles = []

        # Esta parte debe adaptarse según el sitio web
        for link in soup.find_all('a'):
            href = link.get('href')
            title = link.get_text(strip=True)
            if not href or not title or len(title) < 10:
                continue

            categoria = href.strip('/').split('/')[0] if '/' in href else 'general'

            articles.append({
                'categoria': categoria,
                'titular': title,
                'enlace': href
            })

        # Guardar como CSV
        df = pd.DataFrame(articles)
        csv_buffer = df.to_csv(index=False).encode('utf-8')

        # Ruta final
        output_key = f"headlines/final/periodico={periodico}/year={yyyy}/month={mm}/day={dd}/noticias.csv"
        s3.put_object(Bucket=bucket, Key=output_key, Body=csv_buffer)

        return {
            "status": "ok",
            "archivo_procesado": key,
            "salida": output_key
        }

