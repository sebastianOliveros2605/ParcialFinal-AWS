# tests/test_parse.py
import pandas as pd
from jobs import parse_headlines

sample_html = """
<html>
  <body>
    <a href="/deportes/futbol">Noticia de fútbol</a>
    <a href="/economia/inflacion">La inflación sube</a>
    <a href="https://externo.com/noticia">Enlace externo</a>
    <a href="/">Inicio</a>
  </body>
</html>
"""

def test_parse_html_extracts_correct_fields():
    df = parse_headlines.parse_html(sample_html, 'eltiempo')
    assert not df.empty
    assert set(df.columns) == {'categoria', 'titular', 'enlace'}
    assert 'futbol' in df['enlace'][0] or 'futbol' in df['titular'][0]

def test_parse_html_ignores_empty_titles():
    html = '<a href="/deportes/futbol"></a>'
    df = parse_headlines.parse_html(html, 'eltiempo')
    assert df.empty

def test_generate_output_key_format():
    key = parse_headlines.generate_output_key('elespectador', fecha=pd.Timestamp('2024-12-25'))
    assert key == "headlines/final/periodico=elespectador/year=2024/month=12/day=25/noticias.csv"
