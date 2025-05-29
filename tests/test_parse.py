# tests/test_parse_headlines_simple.py

import sys
import os
from datetime import date
import pandas as pd # Necesitarás pandas para la prueba de generate_output_key

# --- Ajusta el path para importar tu módulo ---
# Asume que 'tests' y 'glue_jobs' (o como se llame tu directorio de jobs)
# están al mismo nivel, y ejecutas desde la raíz del proyecto.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'jobs')))

# Importa el módulo que contiene tus funciones
import parse_headlines # type: ignore


# --- HTML de Ejemplo para la Página Principal ---
SAMPLE_MAIN_PAGE_HTML = """
<html>
  <body>
    <h1>Noticias de Prueba</h1>
    <a href="/deportes/futbol-colombiano/evento-importante-123">Evento importante del Fútbol Colombiano</a>
    <a href="/economia/mercados/ultima-hora-mercados-456">Última hora de los mercados</a>
    <a href="https://www.otrositio.com/noticia-externa-789">Noticia de un sitio externo</a>
    <a href="/cultura/cine/estreno-pelicula-012">Estreno de película de cine</a>
    <a href="/nav/nosotros.html">Sobre Nosotros (no es noticia)</a>
    <a href="/">Volver al Inicio</a>
    <a href="/bogota/movilidad-hoy-345">Movilidad Hoy en Bogotá</a>
    <a href="/politica/congreso/debate-clave-678">Debate Clave en el Congreso</a>
  </body>
</html>
"""

# HTML de ejemplo para un artículo (si quisieras probar extract_article_text por separado)
SAMPLE_ARTICLE_CONTENT_HTML = """
<html><body>
    <div class="article-content">
        <p>Este es el primer párrafo del contenido del artículo.</p>
        <p>Y este es el segundo párrafo, con más detalles.</p>
    </div>
</body></html>
"""

# Necesitamos una configuración de PAPERS para que extract_article_text la use,
# incluso si no esperamos que tenga éxito en la llamada de red en estas pruebas.
# Los selectores aquí deberían coincidir con SAMPLE_ARTICLE_CONTENT_HTML si lo usas en una prueba separada.
parse_headlines.PAPERS = {
    'eltiempo': {
        'base_url': 'https://www.eltiempo.com',
        'article_selector': 'div.article-content p' # Selector para SAMPLE_ARTICLE_CONTENT_HTML
    },
    'elespectador': {
        'base_url': 'https://www.elespectador.com',
        'article_selector': 'div.article-body p'
    }
}


def test_parse_main_page_extracts_correct_fields():
    """
    Prueba que parse_main_page_headlines extrae titulares, enlaces y categorías
    del HTML de la página principal. La columna 'texto_completo' contendrá lo que
    extract_article_text devuelva (probablemente vacío o error si las URLs no son reales
    y no se está mockeando la red).
    """
    # Usamos 'eltiempo' como ejemplo, la base_url se usará para completar enlaces relativos.
    df = parse_headlines.parse_main_page_headlines(SAMPLE_MAIN_PAGE_HTML, 'eltiempo')

    # Verificar que se extrajeron algunas filas (las que parecen noticias)
    # Basado en SAMPLE_MAIN_PAGE_HTML, esperamos 5 noticias.
    assert not df.empty, "El DataFrame no debería estar vacío"
    assert len(df) >= 4, f"Se esperaban al menos 4 noticias, se obtuvieron {len(df)}" # Ajusta según tu lógica de filtro

    # Verificar columnas esperadas
    expected_columns = {'categoria', 'titular', 'enlace', 'texto_completo'}
    assert set(df.columns) == expected_columns, f"Las columnas esperadas eran {expected_columns}, se obtuvieron {set(df.columns)}"

    # Verificar contenido de la primera fila (ajusta según el primer enlace que consideres noticia)
    # El primer enlace de noticia es "/deportes/futbol-colombiano/evento-importante-123"
    first_row = df.iloc[0]
    assert first_row['titular'] == "Evento importante del Fútbol Colombiano"
    assert first_row['categoria'] == "deportes" # Basado en la heurística de tu función
    assert "eltiempo.com/deportes/futbol-colombiano/evento-importante-123" in first_row['enlace']
    # Para 'texto_completo', no podemos asegurar mucho sin mockear o tener URLs reales estables.
    # Pero al menos la columna debe existir.
    assert 'texto_completo' in first_row 

    # Verificar que los enlaces que no son noticias (como "Sobre Nosotros", "Volver al Inicio")
    # no estén en el DataFrame o que la cantidad de filas sea la esperada.
    titles = df['titular'].tolist()
    assert "Sobre Nosotros (no es noticia)" not in titles
    assert "Volver al Inicio" not in titles


def test_parse_main_page_ignores_empty_or_very_short_titles():
    """
    Prueba que los enlaces sin texto de título o con títulos muy cortos se ignoran.
    """
    html_no_title = '<html><body><a href="/deportes/futbol-colombiano/sin-titulo-123"></a></body></html>'
    df_no_title = parse_headlines.parse_main_page_headlines(html_no_title, 'eltiempo')
    assert df_no_title.empty, "DataFrame debería estar vacío para enlaces sin título"

    # Asumiendo que tu filtro de longitud de título es > 10
    html_short_title = '<html><body><a href="/deportes/futbol-colombiano/corto-456">Gol</a></body></html>'
    df_short_title = parse_headlines.parse_main_page_headlines(html_short_title, 'eltiempo')
    assert df_short_title.empty, "DataFrame debería estar vacío para enlaces con títulos muy cortos"


def test_parse_main_page_handles_empty_html_input():
    """
    Prueba cómo se maneja una entrada HTML vacía.
    Debería devolver un DataFrame vacío con las columnas correctas.
    """
    df = parse_headlines.parse_main_page_headlines("", 'eltiempo')
    assert df.empty, "DataFrame debería estar vacío para HTML vacío"
    expected_columns = {'categoria', 'titular', 'enlace', 'texto_completo'}
    assert set(df.columns) == expected_columns, "Las columnas deben estar presentes incluso si el DF está vacío"


def test_extract_article_text_with_sample_html():
    """
    Prueba unitaria para extract_article_text usando HTML de ejemplo, sin llamadas de red.
    Para esto, `extract_article_text` tendría que ser modificada para aceptar HTML directamente,
    o esta prueba debe mockear `requests.get` para que devuelva este HTML.
    Si no quieres mockear, esta prueba se vuelve más de integración o necesitas
    una URL pública que SIRVA EXACTAMENTE ESE HTML (difícil).

    Dado que dijiste NO MOCKS, esta prueba es difícil de hacer puramente unitaria
    para extract_article_text si toma una URL.
    La alternativa es probar sólo la lógica de parseo de BeautifulSoup si pudieras
    alimentarle el HTML directamente a la parte de parseo de extract_article_text.

    Aquí, asumimos que parse_headlines.extract_article_text toma una URL.
    Si NO mockeamos, esta prueba en realidad haría una llamada de red a "dummy_url".
    """
    # Esta prueba NO es realmente unitaria para extract_article_text sin mocks si toma una URL.
    # Se incluye para mostrar cómo se intentaría.
    # Necesitaríamos una URL real que devuelva SAMPLE_ARTICLE_CONTENT_HTML
    # o mockear requests.get.

    # Simulemos que tenemos una manera de pasar HTML directamente a la lógica de parseo de `extract_article_text`
    # Esto requeriría refactorizar extract_article_text para separar la obtención de la URL del parseo.
    # Ejemplo de refactorización (NO está en tu código actual de parse_headlines.py):
    # def _parse_article_html_content(article_html, paper_config):
    #     soup = BeautifulSoup(article_html, 'html.parser')
    #     content_elements = soup.select(paper_config['article_selector'])
    #     # ... resto de la lógica de extracción de texto
    #     return " ".join([p.get_text(strip=True) for p in content_elements])

    # Si tuvieras esa función _parse_article_html_content:
    # parsed_text = parse_headlines._parse_article_html_content(SAMPLE_ARTICLE_CONTENT_HTML, parse_headlines.PAPERS['eltiempo'])
    # assert "Este es el primer párrafo" in parsed_text
    # assert "Y este es el segundo párrafo" in parsed_text

    # Como no la tienes, esta prueba de extract_article_text es más conceptual aquí sin mocks.
    # Para ejecutarla realmente, necesitarías una URL que sirva SAMPLE_ARTICLE_CONTENT_HTML
    # o mockear. Si la ejecutas con una URL dummy, fallará o devolverá "".
    texto_extraido = parse_headlines.extract_article_text("http://url.inexistente.para.prueba/articulo.html", parse_headlines.PAPERS['eltiempo'])
    assert texto_extraido == "", "Se espera texto vacío para URL inexistente sin mock"


def test_generate_output_key_format():
    """
    Prueba la función que genera la clave de S3 para la salida.
    Esta es una prueba unitaria pura y no necesita mocks.
    """
    # Usar datetime.date para el argumento fecha como en el código original
    key = parse_headlines.generate_output_key('elespectador', fecha=date(2024, 12, 25))
    assert key == "headlines/final/periodico=elespectador/year=2024/month=12/day=25/noticias.csv"

# Para ejecutar estas pruebas si guardas este archivo (ej. tests/test_parse_simple.py):
# Desde la raíz de tu proyecto: python -m pytest tests/test_parse_simple.py
# O, si no usas pytest y quieres ejecutarlo directamente (necesitarías estructura de unittest.TestCase):
# if __name__ == '__main__':
#   pass # pytest se encarga de descubrir y ejecutar funciones test_*