# glue_jobs/parse_headlines.py
import boto3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date
from io import BytesIO
import re

BUCKET = 'guardar-html'
PAPERS = ['eltiempo', 'elespectador']

def parse_html(html, paper):
    soup = BeautifulSoup(html, 'html.parser')
    headlines = []
    for link in soup.find_all('a', href=True):
        title = link.get_text(strip=True)
        href = link['href']
        if title and re.search(r'/[a-z]+/', href):
            headlines.append({
                'categoria': href.split('/')[1],
                'titular': title,
                'enlace': href if href.startswith('http') else f'https://{paper}.com{href}'
            })
    return pd.DataFrame(headlines)

def load_html_from_s3(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')

def save_dataframe_to_s3(df, bucket, key):
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    boto3.client('s3').put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

def generate_output_key(paper, fecha=None):
    if fecha is None:
        fecha = date.today()
    return f"headlines/final/periodico={paper}/year={fecha.year}/month={fecha.month:02}/day={fecha.day:02}/noticias.csv"

def main():
    today = date.today()
    for paper in PAPERS:
        input_key = f"headlines/raw/{paper}-contenido-{today.isoformat()}.html"
        html = load_html_from_s3(BUCKET, input_key)
        df = parse_html(html, paper)
        output_key = generate_output_key(paper, today)
        save_dataframe_to_s3(df, BUCKET, output_key)

if __name__ == "__main__":
    main()
