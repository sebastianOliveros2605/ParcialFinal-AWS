# tests/test_download.py

import boto3
from moto import mock_s3
from jobs import download_headlines

@mock_s3
def test_s3_guardado_correctamente():
    bucket = 'guardar-html'
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=bucket)

    html = b"<html><head><title>Test</title></head><body>Noticia</body></html>"
    key = download_headlines.generate_s3_key('eltiempo', fecha='2024-01-01')
    download_headlines.save_to_s3(bucket, key, html)

    result = s3.get_object(Bucket=bucket, Key=key)
    content = result['Body'].read()
    assert b"Noticia" in content

def test_urls_son_correctas():
    assert 'https://www.eltiempo.com/' in download_headlines.SITES.values()
    assert 'https://www.elespectador.com/' in download_headlines.SITES.values()

def test_formato_key_s3():
    key = download_headlines.generate_s3_key('elespectador', '2024-05-26')
    assert key == 'headlines/raw/elespectador-contenido-2024-05-26.html'
