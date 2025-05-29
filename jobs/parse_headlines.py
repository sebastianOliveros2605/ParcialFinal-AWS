# glue_jobs/parse_headlines.py
import boto3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date
from io import BytesIO, StringIO # StringIO para to_csv directo a S3
import re
import requests # Para descargar el contenido de cada artículo
import time     # Para ser corteses con los servidores

BUCKET = 'guardar-html' # Asegúrate que este sea tu bucket real
PAPERS = {
    'eltiempo': {
        'base_url': 'https://www.eltiempo.com',
        # IMPORTANTE: Estos selectores son ejemplos y DEBES VERIFICARLOS/AJUSTARLOS
        # inspeccionando el HTML de artículos reales de El Tiempo.
        'article_selector': 'div.main-wrapper-module div.article-content p, div.c-article-content p, div.story_main_content p, div.paywall p'
    },
    'elespectador': {
        'base_url': 'https://www.elespectador.com',
        # IMPORTANTE: Estos selectores son ejemplos y DEBES VERIFICARLOS/AJUSTARLOS
        # inspeccionando el HTML de artículos reales de El Espectador.
        'article_selector': 'div.content-modules p, article.font-article p, div.article-content p'
    }
    # Podrías añadir 'publimetro' aquí si lo usas
}

# Cabeceras para simular un navegador y evitar bloqueos simples
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def get_full_url(href, paper_base_url):
    """Construye una URL completa si es relativa."""
    if href.startswith('http'):
        return href
    if href.startswith('/'):
        return f"{paper_base_url}{href}"
    return f"{paper_base_url}/{href}"

def extract_article_text(article_url, paper_config):
    """
    Descarga el contenido de la URL de un artículo y extrae el texto principal.
    """
    try:
        print(f"Fetching article content from: {article_url}")
        response = requests.get(article_url, headers=REQUEST_HEADERS, timeout=15)
        response.raise_for_status()  # Lanza excepción para errores HTTP 4xx/5xx
        
        article_soup = BeautifulSoup(response.content, 'html.parser')
        
        # Usar el selector CSS específico para el periódico
        # Esto es crucial y puede necesitar ajustes frecuentes si la estructura del sitio cambia
        content_elements = article_soup.select(paper_config['article_selector'])
        
        if not content_elements:
            print(f"Warning: No content found for {article_url} using selector '{paper_config['article_selector']}'")
            return ""

        # Unir el texto de todos los párrafos encontrados
        full_text = "\n".join([p.get_text(separator=" ", strip=True) for p in content_elements])
        return full_text.strip()

    except requests.exceptions.RequestException as e:
        print(f"Error fetching article {article_url}: {e}")
        return ""
    except Exception as e:
        print(f"Error parsing article {article_url}: {e}")
        return ""

def parse_main_page_headlines(main_page_html, paper_key):
    """
    Parsea la página principal para obtener titulares, enlaces y categorías.
    Luego, para cada noticia, obtiene el texto completo del artículo.
    """
    soup = BeautifulSoup(main_page_html, 'html.parser')
    headlines_data = []
    paper_config = PAPERS[paper_key]
    base_url = paper_config['base_url']

    # Este selector de enlaces también puede necesitar ajuste específico por periódico
    # Aquí se asume un enfoque general, pero podría ser más preciso
    # Ejemplo: buscar enlaces dentro de secciones de noticias específicas
    #links = soup.select('article h2 a[href], div.news-item a[href]') # Ejemplo más específico
    links = soup.find_all('a', href=True) # Tu enfoque original, podría ser muy amplio

    processed_urls = set() # Para evitar procesar la misma URL múltiples veces desde la página principal

    for link in links:
        title = link.get_text(strip=True)
        href = link.get('href', '')

        # Filtrar enlaces que no son noticias o son muy genéricos
        if not title or len(title) < 10: # Titulares muy cortos suelen ser navegación
            continue
        
        # Construir URL completa
        full_href = get_full_url(href, base_url)

        if full_href in processed_urls:
            continue
        
        # Heurística para identificar enlaces de noticias (PUEDE NECESITAR MEJORAS)
        # Esto es muy básico. Idealmente, buscarías patrones más específicos en las URLs
        # o te enfocarías en secciones HTML que sabes contienen noticias.
        # Para El Tiempo, las noticias suelen estar en /colombia/, /mundo/, /deportes/, etc.
        # Para El Espectador, patrones como /colombia/, /judicial/, /economia/, etc.
        # El regex r'/[a-z-]+/\d{4,}' podría ser un inicio (sección/año o id largo)
        
        # Una heurística simple para la categoría basada en el primer segmento de la URL relativa
        # Esto puede no ser siempre preciso.
        path_segments = [seg for seg in href.split('/') if seg]
        categoria = "general" # Categoría por defecto
        if len(path_segments) > 0:
            # Intentar normalizar la categoría (ej. 'colombia-actualidad' -> 'colombia')
            # Esto es muy específico del sitio y necesitaría una lógica más robusta
            categoria_raw = path_segments[0].lower()
            if paper_key == 'eltiempo':
                # Lógica específica para El Tiempo si es necesario
                if 'mundo' in categoria_raw: categoria = 'mundo'
                elif 'colombia' in categoria_raw: categoria = 'colombia'
                elif 'bogota' in categoria_raw: categoria = 'bogota'
                elif 'deportes' in categoria_raw: categoria = 'deportes'
                # ... más categorías
                else: categoria = categoria_raw # o mantener general
            elif paper_key == 'elespectador':
                # Lógica específica para El Espectador
                if 'judicial' in categoria_raw: categoria = 'judicial'
                elif 'politica' in categoria_raw: categoria = 'politica'
                # ... más categorías
                else: categoria = categoria_raw
            else:
                categoria = categoria_raw
        
        # Considerar un enlace como noticia si tiene una "categoría" aparente y no es un enlace interno simple
        # Podrías añadir filtros más estrictos, como verificar que la URL no apunte a secciones como "horoscopo", "clasificados", etc.
        # O que el título no sea algo como "Contáctenos", "Términos y condiciones"
        is_news_link = re.search(r'/[a-zA-Z0-9-]+(?:/\d{4,}|/[a-zA-Z0-9-]+)', href) # Patrón un poco más restrictivo

        if title and is_news_link and full_href not in processed_urls:
            print(f"Found potential news: '{title}' - Link: {full_href}")
            
            # Obtener el texto completo del artículo
            # Añadimos un pequeño delay para ser corteses con el servidor
            time.sleep(1) # Espera 1 segundo entre cada petición de artículo
            texto_completo = extract_article_text(full_href, paper_config)
            
            if texto_completo: # Solo añadir si obtuvimos contenido
                headlines_data.append({
                    'categoria': categoria,
                    'titular': title,
                    'enlace': full_href,
                    'texto_completo': texto_completo # Nueva columna
                })
                processed_urls.add(full_href)
            else:
                print(f"Skipping article due to empty content: {full_href}")

    if not headlines_data:
        print(f"Warning: No headlines extracted for {paper_key}. Check selectors and page structure.")
        # Devolver un DataFrame vacío con las columnas esperadas para evitar errores posteriores
        return pd.DataFrame(columns=['categoria', 'titular', 'enlace', 'texto_completo'])
        
    return pd.DataFrame(headlines_data)

def load_html_from_s3(bucket, key):
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error loading {key} from S3: {e}")
        raise # Relanzar la excepción para que el job falle si no puede cargar el HTML

def save_dataframe_to_s3(df, bucket, key):
    """Guarda el DataFrame en S3 como CSV."""
    if df.empty:
        print(f"DataFrame is empty. Skipping save to S3 for key: {key}")
        return

    print(f"Saving DataFrame with {len(df)} rows to S3: s3://{bucket}/{key}")
    # Usar StringIO para escribir el CSV en memoria y luego subirlo
    csv_buffer = StringIO()
    # Asegúrate de que las columnas estén en el orden deseado y que se incluyan los encabezados
    df.to_csv(csv_buffer, index=False, header=True) # header=True es importante para Glue Crawler
    
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print("Save complete.")


def generate_output_key(paper, fecha=None):
    if fecha is None:
        fecha = date.today()
    # El nombre del archivo puede ser simplemente data.csv o noticias.csv
    return f"headlines/final/periodico={paper}/year={fecha.year}/month={fecha.month:02}/day={fecha.day:02}/noticias.csv"

def main():
    today = date.today()
    for paper_key in PAPERS.keys(): # Iterar sobre las claves del diccionario PAPERS
        input_key = f"headlines/raw/{paper_key}-contenido-{today.isoformat()}.html"
        print(f"Processing {paper_key} from {input_key}")
        
        try:
            html_content = load_html_from_s3(BUCKET, input_key)
        except Exception as e:
            print(f"Failed to load HTML for {paper_key}. Skipping this paper. Error: {e}")
            continue # Pasar al siguiente periódico si falla la carga

        if not html_content:
            print(f"No HTML content found for {paper_key}. Skipping.")
            continue

        df = parse_main_page_headlines(html_content, paper_key)
        
        if not df.empty:
            output_key = generate_output_key(paper_key, today)
            save_dataframe_to_s3(df, BUCKET, output_key)
        else:
            print(f"No data extracted for {paper_key} on {today.isoformat()}. No CSV will be saved.")

if __name__ == "__main__":
    # Para pruebas locales, podrías simular los argumentos de Glue o cargar un HTML local
    # Ejemplo de simulación de argumentos para un job de Glue (si los usaras)
    # import sys
    # sys.argv.extend([
    #     '--JOB_NAME', 'mi_job_de_parseo',
    #     '--BUCKET', 'mi-bucket-real-de-glue', # Podrías pasar el bucket como argumento
    # ])
    # args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET']) # Si usas utils de Glue
    # BUCKET = args['BUCKET']
    main()