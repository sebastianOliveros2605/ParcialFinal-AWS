# tests/test_parse_headlines.py

import sys
import os
import unittest
import requests
from unittest.mock import patch, MagicMock # Para mockear llamadas de red
from datetime import date
# Asegurarse de que el directorio 'glue_jobs' (o donde esté 'parse_headlines.py') esté en el PYTHONPATH
# Esto asume que 'tests' y 'glue_jobs' están al mismo nivel, y ejecutas las pruebas desde el directorio raíz del proyecto.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importar el módulo que queremos probar DESPUÉS de ajustar el path
import parse_headlines # type: ignore

# --- Sample Data para Pruebas ---
SAMPLE_MAIN_PAGE_HTML_ELTIEMPO = """
<html>
  <body>
    <h1>El Tiempo</h1>
    <a href="/deportes/futbol-colombiano/millonarios-gano-ayer-12345">Millonarios ganó ayer</a>
    <a href="/economia/inflacion-en-colombia-sigue-alta-67890">La inflación en Colombia sigue alta</a>
    <a href="https://www.eltiempo.com/mundo/guerra-ucrania-rusia-situacion-actual-00112">Guerra Ucrania Rusia</a>
    <a href="/cultura/conciertos-bogota-2024-00113">Conciertos en Bogotá</a>
    <a href="/opinion/columnistas/fulanito-perez/mi-columna-semanal-00114">Columna de Fulanito</a>
    <a href="/servicios/horoscopo">Horóscopo (no es noticia)</a>
    <a href="/">Ir al Inicio</a>
    <a href="/bogota/trancones-hoy-123">Trancones en Bogotá (Noticia de Bogotá)</a>
  </body>
</html>
"""

SAMPLE_ARTICLE_HTML_DEPORTES = """
<html>
<body>
  <div class="article-content">
    <p>Este es el texto completo del artículo de deportes sobre Millonarios.</p>
    <p>Tuvo un gran desempeño.</p>
  </div>
</body>
</html>
"""

SAMPLE_ARTICLE_HTML_ECONOMIA = """
<html>
<body>
  <article class="font-article">
    <p>Detalles sobre la inflación y cómo afecta la economía colombiana.</p>
    <p>Los precios de la canasta familiar subieron.</p>
  </article>
</body>
</html>
"""

# Configuración de PAPERS simplificada para pruebas
# Los selectores aquí deben coincidir con SAMPLE_ARTICLE_HTML_*
TEST_PAPERS_CONFIG = {
    'eltiempo': {
        'base_url': 'https://www.eltiempo.com',
        'article_selector': 'div.article-content p, article.font-article p' # Cubre ambos ejemplos
    },
    'elespectador': { # Añadimos una config básica aunque no la usemos en todos los tests
        'base_url': 'https://www.elespectador.com',
        'article_selector': 'div.article-body p'
    }
}


class TestParseHeadlines(unittest.TestCase):

    def test_generate_output_key_format(self):
        # Esta prueba no necesita cambios ya que la función no cambió su lógica fundamental
        key = parse_headlines.generate_output_key('elespectador', fecha=date(2024, 12, 25))
        self.assertEqual(key, "headlines/final/periodico=elespectador/year=2024/month=12/day=25/noticias.csv")

    @patch('parse_headlines.requests.get') # Mockea requests.get dentro del módulo parse_headlines
    def test_extract_article_text_success(self, mock_requests_get):
        # Configurar el mock para devolver una respuesta exitosa con HTML de ejemplo
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = SAMPLE_ARTICLE_HTML_DEPORTES.encode('utf-8')
        mock_response.raise_for_status = MagicMock() # para que no falle en response.raise_for_status()
        mock_requests_get.return_value = mock_response

        test_url = "https://www.eltiempo.com/deportes/algun-articulo"
        # Usamos una config de prueba para el selector
        extracted_text = parse_headlines.extract_article_text(test_url, TEST_PAPERS_CONFIG['eltiempo'])

        mock_requests_get.assert_called_once_with(test_url, headers=parse_headlines.REQUEST_HEADERS, timeout=15)
        expected_text = "Este es el texto completo del artículo de deportes sobre Millonarios.\nTuvo un gran desempeño."
        self.assertEqual(extracted_text, expected_text)

    @patch('parse_headlines.requests.get')
    def test_extract_article_text_request_error(self, mock_requests_get):
        # Configurar el mock para simular un error de red
        mock_requests_get.side_effect = requests.exceptions.RequestException("Network error")

        test_url = "https://www.eltiempo.com/deportes/otro-articulo"
        extracted_text = parse_headlines.extract_article_text(test_url, TEST_PAPERS_CONFIG['eltiempo'])

        self.assertEqual(extracted_text, "") # Esperamos string vacío en caso de error

    @patch('parse_headlines.requests.get')
    def test_extract_article_text_no_selector_match(self, mock_requests_get):
        # Respuesta exitosa, pero el selector no encontrará nada
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = "<html><body><p>Contenido irrelevante</p></body></html>".encode('utf-8')
        mock_response.raise_for_status = MagicMock()
        mock_requests_get.return_value = mock_response

        test_url = "https://www.eltiempo.com/deportes/articulo-raro"
        # Usamos una config de prueba para el selector
        extracted_text = parse_headlines.extract_article_text(test_url, TEST_PAPERS_CONFIG['eltiempo'])
        self.assertEqual(extracted_text, "")


    # Patch para el PAPERS global y para extract_article_text
    # Esto es más complejo porque parse_main_page_headlines llama a extract_article_text internamente
    @patch('parse_headlines.PAPERS', TEST_PAPERS_CONFIG) # Sobreescribe el PAPERS global
    @patch('parse_headlines.extract_article_text') # Mockea la función que hace la llamada de red real
    def test_parse_main_page_headlines_extracts_correct_fields(self, mock_extract_article_text):
        # Definimos lo que retornará el mock de extract_article_text
        # Podemos hacerlo más inteligente si es necesario, pero para este test, un valor fijo es suficiente
        mock_extract_article_text.return_value = "Texto completo del artículo mockeado."

        df = parse_headlines.parse_main_page_headlines(SAMPLE_MAIN_PAGE_HTML_ELTIEMPO, 'eltiempo')

        self.assertFalse(df.empty)
        self.assertEqual(set(df.columns), {'categoria', 'titular', 'enlace', 'texto_completo'})

        # Verificar que se llamó a extract_article_text para los enlaces de noticias
        # (Millonarios, Inflación, Guerra, Conciertos, Columna, Trancones) -> 6 llamadas esperadas
        # El filtro de is_news_link en parse_main_page_headlines es clave aquí
        # Basado en SAMPLE_MAIN_PAGE_HTML_ELTIEMPO y la heurística actual de is_news_link
        # y los filtros de título, esperamos que se intente extraer texto para estos:
        expected_calls_for_articles = [
            "https://www.eltiempo.com/deportes/futbol-colombiano/millonarios-gano-ayer-12345",
            "https://www.eltiempo.com/economia/inflacion-en-colombia-sigue-alta-67890",
            "https://www.eltiempo.com/mundo/guerra-ucrania-rusia-situacion-actual-00112",
            "https://www.eltiempo.com/cultura/conciertos-bogota-2024-00113",
            "https://www.eltiempo.com/opinion/columnistas/fulanito-perez/mi-columna-semanal-00114",
            "https://www.eltiempo.com/bogota/trancones-hoy-123"
        ]
        self.assertGreaterEqual(mock_extract_article_text.call_count, 5) # Puede ser 6 si el filtro de columna pasa
        
        # Verificamos el contenido de una fila
        # La primera noticia es de Millonarios
        self.assertEqual(df.iloc[0]['titular'], 'Millonarios ganó ayer')
        self.assertEqual(df.iloc[0]['categoria'], 'deportes') # Basado en la heurística de la URL
        self.assertTrue('millonarios-gano-ayer' in df.iloc[0]['enlace'])
        self.assertEqual(df.iloc[0]['texto_completo'], "Texto completo del artículo mockeado.")

        # Comprobar que los enlaces que no son noticias se filtran (ej. "Horóscopo", "Ir al Inicio")
        # Esto se comprueba indirectamente porque no deberían estar en el df ni generar llamadas a extract_article_text.
        # Y porque el número de filas en df debería ser igual al número de artículos válidos.
        self.assertEqual(len(df), mock_extract_article_text.call_count)


    @patch('parse_headlines.PAPERS', TEST_PAPERS_CONFIG)
    @patch('parse_headlines.extract_article_text')
    def test_parse_main_page_headlines_ignores_empty_titles_or_bad_links(self, mock_extract_article_text):
        # Simula que extract_article_text siempre devuelve algo para no fallar por eso.
        mock_extract_article_text.return_value = "Mocked content."

        html_no_title = '<html><body><a href="/deportes/futbol"></a></body></html>'
        df_no_title = parse_headlines.parse_main_page_headlines(html_no_title, 'eltiempo')
        self.assertTrue(df_no_title.empty)
        mock_extract_article_text.assert_not_called() # No debería llamar si el título está vacío

        mock_extract_article_text.reset_mock() # Resetear mock para la siguiente prueba

        html_short_title = '<html><body><a href="/deportes/futbol">Gol</a></body></html>' # Título muy corto
        df_short_title = parse_headlines.parse_main_page_headlines(html_short_title, 'eltiempo')
        self.assertTrue(df_short_title.empty) # Asume que "Gol" es muy corto y se filtra
        mock_extract_article_text.assert_not_called()

        mock_extract_article_text.reset_mock()

        # Un enlace que no parece de noticia según la heurística actual
        html_non_news_link = '<html><body><a href="/contactenos">Contáctenos ahora mismo</a></body></html>'
        df_non_news = parse_headlines.parse_main_page_headlines(html_non_news_link, 'eltiempo')
        self.assertTrue(df_non_news.empty)
        mock_extract_article_text.assert_not_called()

    @patch('parse_headlines.PAPERS', TEST_PAPERS_CONFIG)
    @patch('parse_headlines.extract_article_text')
    def test_parse_main_page_headlines_empty_html(self, mock_extract_article_text):
        df = parse_headlines.parse_main_page_headlines("", 'eltiempo')
        self.assertTrue(df.empty)
        self.assertEqual(set(df.columns), {'categoria', 'titular', 'enlace', 'texto_completo'}) # Verifica que las columnas se creen aunque esté vacío
        mock_extract_article_text.assert_not_called()

if __name__ == '__main__':
    unittest.main()