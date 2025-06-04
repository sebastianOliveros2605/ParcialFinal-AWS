import boto3

glue = boto3.client('glue')

# Nombre del crawler que ya existe en Glue
CRAWLER_NAME = 'crawler-noticias-final'

def handler(event=None, context=None):
    try:
        glue.start_crawler(Name=CRAWLER_NAME)
        return {"status": "ok", "mensaje": f"Crawler '{CRAWLER_NAME}' ejecutado correctamente"}
    except glue.exceptions.CrawlerRunningException:
        return {"status": "error", "mensaje": "Crawler ya está en ejecución"}
    except Exception as e:
        return {"status": "error", "mensaje": str(e)}

