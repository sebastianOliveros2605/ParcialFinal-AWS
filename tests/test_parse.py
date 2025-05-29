# tests/test_parse_headlines.py

import sys
import os
from datetime import date
import pandas as pd
from unittest.mock import patch, MagicMock # Seguiremos necesitando mock para requests en get_article_text

# --- Ajusta el path para importar tu módulo ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'jobs')))

import parse_headlines # type: ignore
import requests
# --- HTML de Ejemplo para la Página Principal ---
SAMPLE_MAIN_PAGE_HTML_ELTIEMPO = """
<html>
  <body>
    <h1>El Tiempo - Página de Prueba</h1>
    <a href="/deportes/futbol-colombiano/articulo-prueba-deportes-123">Artículo de Deportes Prueba (Título Largo)</a>
    <a href="/economia/inflacion-colombia/articulo-prueba-economia-456">Artículo de Economía Prueba (Otro Título)</a>
    <a href="/servicios/horoscopo/hoy">Horóscopo de Hoy (No Noticia por Path)</a>
    <a href="/cultura/evento-cultural-789">Evento Cultural Importante</a>
    <a href="/opinion/columnista-abc/columna-semanal-000">Columna Semanal de Opinión</a>
    <a href="/nav/contacto">Contacto (No Noticia por Path)</a>
    <a href="/tecnologia/gadgets/nuevo-lanzamiento-xyz">Nuevo Gadget Tecnológico Presentado</a>
    <a href="/bogota/trancon-calle-80-hoy">Trancón en la Calle 80 hoy en Bogotá</a>
    <a href="/mundo/conflicto-internacional-detalle-111">Detalles del Conflicto Internacional Actual</a>
    <a href="/noticia-corta">Título corto</a>
  </body>
</html>
"""

# --- HTML de Ejemplo para un Artículo Individual ---
SAMPLE_ARTICLE_CONTENT_HTML = """
<html><body>
    <div class="article-content">
        <p>Este es el primer párrafo del contenido del artículo de prueba.</p>
        <p>Y este es el segundo párrafo, con más detalles jugosos.</p>
    </div>
    <div class="paywall">
        <p>Contenido detrás de un paywall también.</p>
    </div>
</body></html>
"""
TEST_ARTICLE_CONFIG = {
    'eltiempo': {
        'base_url': 'https://www.eltiempo.com',
        'article_selector': 'div.article-content p, div.paywall p' # Selector para SAMPLE_ARTICLE_CONTENT_HTML
    },
    'elespectador': {
        'base_url': 'https://www.elespectador.com',
        'article_selector': 'div.story-content p' # Un selector de ejemplo
    }
}

# Sobrescribir el ARTICLE_CONFIG global en el módulo parse_headlines ANTES de que se ejecuten los tests
# Esto asegura que las funciones usen nuestra configuración de prueba.
parse_headlines.ARTICLE_CONFIG = TEST_ARTICLE_CONFIG


# ----- Pruebas para get_article_text -----
@patch('parse_headlines.requests.get') # Mockear la llamada de red real
def test_get_article_text_success(mock_requests_get):
    """Prueba la extracción de texto exitosa cuando requests.get funciona."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = SAMPLE_ARTICLE_CONTENT_HTML.encode('utf-8')
    mock_response.raise_for_status = MagicMock()
    mock_requests_get.return_value = mock_response

    test_url = "https://www.eltiempo.com/deportes/articulo-real-o-simulado"
    # Usamos la configuración de 'eltiempo' de TEST_ARTICLE_CONFIG
    extracted_text = parse_headlines.get_article_text(test_url, 'eltiempo')

    mock_requests_get.assert_called_once_with(test_url, headers=parse_headlines.REQUEST_HEADERS, timeout=10)
    expected_text = "Este es el primer párrafo del contenido del artículo de prueba.\nY este es el segundo párrafo, con más detalles jugosos.\nContenido detrás de un paywall también."
    assert extracted_text == expected_text

@patch('parse_headlines.requests.get')
def test_get_article_text_request_fails(mock_requests_get):
    """Prueba que se devuelve vacío si requests.get falla."""
    mock_requests_get.side_effect = requests.exceptions.RequestException("Simulated network error")
    
    extracted_text = parse_headlines.get_article_text("https://www.eltiempo.com/error/url-falla", 'eltiempo')
    assert extracted_text == ""

@patch('parse_headlines.requests.get')
def test_get_article_text_no_selector_match(mock_requests_get):
    """Prueba que se devuelve vacío si el selector CSS no encuentra nada."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    # HTML que no coincide con 'div.article-content p, div.paywall p'
    mock_response.content = "<html><body><p>Texto en un lugar incorrecto.</p></body></html>".encode('utf-8')
    mock_response.raise_for_status = MagicMock()
    mock_requests_get.return_value = mock_response

    extracted_text = parse_headlines.get_article_text("https://www.eltiempo.com/contenido/sin-selector", 'eltiempo')
    assert extracted_text == ""

def test_get_article_text_unknown_paper_key():
    """Prueba que devuelve vacío si la clave del periódico no está en ARTICLE_CONFIG."""
    texto = parse_headlines.get_article_text("http://someurl.com/article", "periodico_desconocido")
    assert texto == ""

@patch('parse_headlines.time.sleep', MagicMock()) # Mockear time.sleep para acelerar tests
def test_parse_html_extracts_main_fields_and_empty_text():
    """
    Prueba que parse_html extrae titulares, enlaces y categorías de la página principal.
    'texto_completo' será vacío porque las URLs de prueba darán 404 y no mockeamos la red aquí.
    """
    df = parse_headlines.parse_html(SAMPLE_MAIN_PAGE_HTML_ELTIEMPO, 'eltiempo')

    assert not df.empty, "El DataFrame no debería estar vacío"
    
    # Basado en SAMPLE_MAIN_PAGE_HTML_ELTIEMPO y los filtros en parse_html:
    # - "Artículo de Deportes Prueba (Título Largo)" -> SÍ
    # - "Artículo de Economía Prueba (Otro Título)" -> SÍ
    # - "Horóscopo de Hoy (No Noticia por Path)" -> NO (excluido por path '/servicios/')
    # - "Evento Cultural Importante" -> SÍ
    # - "Columna Semanal de Opinión" -> SÍ
    # - "Contacto (No Noticia por Path)" -> NO (excluido por path '/nav/')
    # - "Nuevo Gadget Tecnológico Presentado" -> SÍ
    # - "Trancón en la Calle 80 hoy en Bogotá" -> SÍ
    # - "Detalles del Conflicto Internacional Actual" -> SÍ
    # - "Título corto" -> NO (excluido por longitud de título < 15)
    # Total esperado: 7 noticias
    expected_news_count = 7
    assert len(df) == expected_news_count, \
        f"Se esperaban {expected_news_count} noticias, se obtuvieron {len(df)}. Títulos: {df['titular'].tolist()}"

    expected_columns = {'categoria', 'titular', 'enlace', 'texto_completo'}
    assert set(df.columns) == expected_columns

    # Verificar la primera fila extraída
    first_row = df.iloc[0]
    assert first_row['titular'] == "Artículo de Deportes Prueba (Título Largo)"
    assert first_row['categoria'] == "deportes" # Basado en la heurística
    assert "eltiempo.com/deportes/futbol-colombiano/articulo-prueba-deportes-123" in first_row['enlace']
    assert first_row['texto_completo'] == "", "texto_completo debería ser vacío ya que la URL no existe"

    # Verificar que los títulos filtrados no están
    titles_extracted = df['titular'].tolist()
    assert "Horóscopo de Hoy (No Noticia por Path)" not in titles_extracted
    assert "Contacto (No Noticia por Path)" not in titles_extracted
    assert "Título corto" not in titles_extracted


@patch('parse_headlines.time.sleep', MagicMock())
def test_parse_html_handles_empty_input_html():
    """Prueba que devuelve un DataFrame vacío con columnas si el HTML de entrada está vacío."""
    df = parse_headlines.parse_html("", 'eltiempo')
    assert df.empty
    expected_columns = {'categoria', 'titular', 'enlace', 'texto_completo'}
    assert set(df.columns) == expected_columns

@patch('parse_headlines.time.sleep', MagicMock())
def test_parse_html_unknown_paper_key():
    """Prueba que devuelve un DataFrame vacío si la clave del periódico es desconocida."""
    df = parse_headlines.parse_html(SAMPLE_MAIN_PAGE_HTML_ELTIEMPO, "periodico_fantasma")
    assert df.empty
    expected_columns = {'categoria', 'titular', 'enlace', 'texto_completo'}
    assert set(df.columns) == expected_columns


# ----- Prueba para generate_output_key -----
def test_generate_output_key_format():
    """Prueba la función que genera la clave de S3."""
    key = parse_headlines.generate_output_key('elespectador', fecha=date(2024, 12, 25))
    assert key == "headlines/final/periodico=elespectador/year=2024/month=12/day=25/noticias.csv"