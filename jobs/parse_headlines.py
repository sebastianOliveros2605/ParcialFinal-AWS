# glue_jobs/parse_headlines.py
import boto3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date
from io import StringIO # Usar StringIO para to_csv a S3 es más directo
import re
import requests # Para descargar el contenido de cada artículo
import time     # Para ser corteses con los servidores

BUCKET = 'guardar-html' # Asegúrate que este sea tu bucket real

# Configuración simplificada para la extracción de texto del artículo
# ¡IMPORTANTE! Estos selectores son ejemplos y DEBES VERIFICARLOS y AJUSTARLOS
# inspeccionando el HTML de artículos reales de los periódicos.
ARTICLE_CONFIG = {
    'eltiempo': {
        'base_url': 'https://www.eltiempo.com',
        'article_selector': 'div.article-content p, div.story-content p, div.paywall p'
    },
    'elespectador': {
        'base_url': 'https://www.elespectador.com',
        'article_selector': 'div.content-modules p, article.font-article p, div.article-content p'
    }
    # Si usas publimetro, añádelo aquí
}

REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def get_article_text(article_url, paper_key):
    """Descarga y extrae el texto principal de un artículo."""
    if paper_key not in ARTICLE_CONFIG:
        print(f"Warning: No article config found for paper '{paper_key}'. Skipping text extraction for {article_url}")
        return ""
        
    config = ARTICLE_CONFIG[paper_key]
    try:
        # Asegurarse de que la URL sea completa
        if not article_url.startswith('http'):
            article_url = f"{config['base_url']}{article_url if article_url.startswith('/') else '/' + article_url}"

        print(f"Fetching article content from: {article_url} for paper {paper_key}")
        response = requests.get(article_url, headers=REQUEST_HEADERS, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        paragraphs = soup.select(config['article_selector'])
        
        if not paragraphs:
            print(f"Warning: No text found for {article_url} using selector '{config['article_selector']}'")
            return ""
            
        return "\n".join([p.get_text(separator=" ", strip=True) for p in paragraphs]).strip()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching article {article_url}: {e}")
        return ""
    except Exception as e:
        print(f"Error parsing or processing article {article_url}: {e}")
        return ""

def parse_html(html_content_main_page, paper_key): # Renombrado el primer argumento para claridad
    """
    Parsea la página principal, extrae titulares, y luego el texto completo de cada artículo.
    """
    soup = BeautifulSoup(html_content_main_page, 'html.parser')
    headlines_data = []
    
    if paper_key not in ARTICLE_CONFIG:
        print(f"Error: Configuration for paper '{paper_key}' not found in ARTICLE_CONFIG.")
        return pd.DataFrame(columns=['categoria', 'titular', 'enlace', 'texto_completo'])

    paper_base_url = ARTICLE_CONFIG[paper_key]['base_url']

    for link_tag in soup.find_all('a', href=True):
        title = link_tag.get_text(strip=True)
        href = link_tag['href']

        # Filtros básicos para identificar enlaces de noticias
        if not title or len(title) < 15: # Títulos muy cortos suelen ser navegación
            continue
        if not re.search(r'/[a-zA-Z0-9-]+(?:/\d{4,}|/[a-zA-Z0-9-]+)', href): # Patrón de URL de noticia
            continue
        EXCLUDE_URL_PATHS = ['/navegacion/', '/servicios/', '/horoscopo/', '/nav/'] # <--- Añadido '/nav/'
        if any(excluded_path in href.lower() for excluded_path in EXCLUDE_URL_PATHS):
            print(f"Skipping non-news link based on URL path: {href}")
            continue
        # Construir URL completa para el artículo
        article_full_url = href
        if not href.startswith('http'):
            article_full_url = f"{paper_base_url}{href if href.startswith('/') else '/' + href}"
        
        # Extraer categoría (heurística simple, puede necesitar mejora)
        path_segments = [seg for seg in href.split('/') if seg]
        categoria = path_segments[0] if path_segments else "general"

        # Obtener el texto completo del artículo
        print(f"Processing article: {title} - {article_full_url}")
        time.sleep(0.5) # Pequeño delay para ser cortés
        texto_completo = get_article_text(article_full_url, paper_key) # Pasar paper_key para config

        headlines_data.append({
            'categoria': categoria,
            'titular': title,
            'enlace': article_full_url, # Guardar la URL completa
            'texto_completo': texto_completo
        })

    if not headlines_data:
        print(f"Warning: No headlines extracted for {paper_key}. Check HTML structure and selectors.")
        return pd.DataFrame(columns=['categoria', 'titular', 'enlace', 'texto_completo'])
        
    return pd.DataFrame(headlines_data)

def load_html_from_s3(bucket, key):
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error loading {key} from S3: {e}")
        raise

def save_dataframe_to_s3(df, bucket, key):
    if df.empty:
        print(f"DataFrame is empty. Skipping save to S3 for key: s3://{bucket}/{key}")
        return

    print(f"Saving DataFrame with {len(df)} rows to S3: s3://{bucket}/{key}")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=True) # Es bueno tener header para Glue Crawler
    
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print("Save complete.")

def generate_output_key(paper, fecha=None):
    if fecha is None:
        fecha = date.today()
    return f"headlines/final/periodico={paper}/year={fecha.year}/month={fecha.month:02}/day={fecha.day:02}/noticias.csv"

def main():
    today = date.today()
    # PAPERS ahora se define globalmente, pero el main iterará sobre las claves de ARTICLE_CONFIG
    # para asegurar que tenemos configuración para cada periódico.
    for paper_name in ARTICLE_CONFIG.keys():
        print(f"\n--- Processing: {paper_name} ---")
        input_key = f"headlines/raw/{paper_name}-contenido-{today.isoformat()}.html"
        
        try:
            html_main_page = load_html_from_s3(BUCKET, input_key)
        except Exception as e:
            print(f"Failed to load main page HTML for {paper_name} (s3://{BUCKET}/{input_key}). Skipping. Error: {e}")
            continue
        
        df = parse_html(html_main_page, paper_name) # Pasar paper_name como paper_key
        
        if not df.empty:
            output_key = generate_output_key(paper_name, today)
            save_dataframe_to_s3(df, BUCKET, output_key)
        else:
            print(f"No data extracted for {paper_name}. No CSV will be saved for {today.isoformat()}.")

if __name__ == "__main__":
    main()