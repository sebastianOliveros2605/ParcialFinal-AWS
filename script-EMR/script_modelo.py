# news_classification_script.py

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer, IndexToString
from pyspark.ml.classification import RandomForestClassifier # Cambiado de LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import numpy as np # Asegúrate de que numpy esté disponible en el entorno Spark

def main():
    # Celda 1: Inicializar SparkSession
    # =================================================
    print("Celda 1: Inicializando SparkSession...")

    spark = SparkSession.builder.appName("NewsCategoryClassificationEMRScript") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    print("SparkSession iniciada.")

    # Celda 2: Parámetros de Configuración
    # ====================================================
    print("Celda 2: Definiendo parámetros de configuración...")

    GLUE_DATABASE = "parcial-final"  # Nombre de tu base de datos en Glue Data Catalog
    GLUE_TABLE_NAME = "final"        # Nombre de tu tabla de noticias en Glue Data Catalog
    S3_OUTPUT_PATH = "s3://resulta2-emr/resultados-modelo/" # Ruta s3 para guardar predicciones

    print(f"  Glue Database: {GLUE_DATABASE}")
    print(f"  Glue Table: {GLUE_TABLE_NAME}")
    print(f"  S3 Output Path: {S3_OUTPUT_PATH}")

    # Celda 3: Cargar Datos desde AWS Glue Data Catalog
    # ================================================
    print("Celda 3: Cargando datos desde AWS Glue Data Catalog...")

    try:
        table_identifier = f"`{GLUE_DATABASE}`.`{GLUE_TABLE_NAME}`"
        print(f"Intentando leer de la tabla: {table_identifier}")
        
        news_df_raw = spark.read.table(table_identifier)
        
        print("Esquema de los datos cargados:")
        news_df_raw.printSchema()
        
        initial_count = news_df_raw.count()
        print(f"Número de filas cargadas inicialmente: {initial_count}")
        if initial_count == 0:
            print("ERROR: No se cargaron datos desde Glue. El script no puede continuar.")
            spark.stop()
            sys.exit(1)
        news_df_raw.show(5, truncate=False)

    except Exception as e:
        print(f"ERROR al cargar datos desde Glue Data Catalog: {e}")
        print("Verifica:")
        print("  1. Que el clúster EMR tenga la configuración 'spark-hive-site' para usar Glue Catalog.")
        print("  2. Que el rol IAM del clúster EMR tenga permisos para glue:GetTable, glue:GetPartitions, etc. y s3:GetObject para los datos subyacentes.")
        print("  3. Que la base de datos y tabla existan y los nombres sean correctos.")
        spark.stop()
        sys.exit(1) # Terminar el script con error

    # Celda 4: Preprocesamiento Básico de Datos y Limpieza de Categorías
    # =================================================================
    print("Celda 4: Realizando preprocesamiento básico de datos y limpieza de categorías...")

    COL_TEXT_ORIGINAL = "titular" 
    COL_LABEL_ORIGINAL = "categoria" 

    if COL_TEXT_ORIGINAL not in news_df_raw.columns or COL_LABEL_ORIGINAL not in news_df_raw.columns:
        print(f"ERROR: Las columnas esperadas '{COL_TEXT_ORIGINAL}' o '{COL_LABEL_ORIGINAL}' no se encuentran en el DataFrame.")
        print("Columnas disponibles:", news_df_raw.columns)
        print("Revisa los nombres de las columnas en tu tabla de Glue. Es común que el crawler las ponga en minúscula.")
        spark.stop()
        sys.exit(1)

    news_df_intermediate = news_df_raw.select(
            F.col(COL_TEXT_ORIGINAL).alias("TITULAR_orig"),
            F.col(COL_LABEL_ORIGINAL).alias("CATEGORIA_orig")
        ) \
        .filter(F.col("TITULAR_orig").isNotNull() & F.col("CATEGORIA_orig").isNotNull()) \
        .filter(F.length(F.trim(F.col("TITULAR_orig"))) > 0) \
        .filter(F.length(F.trim(F.col("CATEGORIA_orig"))) > 0)

    print(f"Filas después del filtrado inicial de nulos/vacíos: {news_df_intermediate.count()}")

    print("Limpiando categorías no deseadas (ej. URLs)...")
    news_df_cleaned_categories = news_df_intermediate.withColumn(
        "CATEGORIA_limpia",
        F.lower(F.trim(F.col("CATEGORIA_orig")))
    )

    categorias_a_eliminar_patrones = ["http:", "https:", "www."] 
    condicion_filtro = F.lit(True)
    for patron in categorias_a_eliminar_patrones:
        condicion_filtro = condicion_filtro & (~F.col("CATEGORIA_limpia").startswith(patron))

    news_df = news_df_cleaned_categories.filter(condicion_filtro) \
        .select(
            F.col("TITULAR_orig").alias("TITULAR"),
            F.col("CATEGORIA_limpia").alias("CATEGORIA")
        )

    count_after_category_cleaning = news_df.count()
    if count_after_category_cleaning == 0:
        print("No quedaron datos después de la limpieza de categorías. Verifica tus datos o los patrones de limpieza.")
        spark.stop()
        sys.exit(1)

    print(f"Número de filas después de la limpieza de categorías: {count_after_category_cleaning}")
    print("Distribución de categorías después de la limpieza:")
    news_df.groupBy("CATEGORIA").count().orderBy(F.desc("count")).show(50)
    news_df.show(5, truncate=False)

    # Celda 5: Definición del Pipeline de Machine Learning
    # =================================================
    print("Celda 5: Definiendo el pipeline de Machine Learning...")

    label_indexer = StringIndexer(inputCol="CATEGORIA", outputCol="label_index", handleInvalid="skip")
    tokenizer = Tokenizer(inputCol="TITULAR", outputCol="words")
    stopwords_es = StopWordsRemover.loadDefaultStopWords("spanish")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stopwords_es)
    hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=20000)
    idf = IDF(inputCol="raw_features", outputCol="features")
    classifier = RandomForestClassifier(featuresCol="features", labelCol="label_index", numTrees=100, seed=42)
    label_converter = IndexToString(inputCol="prediction", outputCol="predicted_CATEGORIA")

    pipeline_stages_initial = [label_indexer, tokenizer, remover, hashing_tf, idf, classifier]
    pipeline = Pipeline(stages=pipeline_stages_initial)
    print("Pipeline de ML definido.")

    # Celda 6: Dividir Datos, Entrenar el Modelo y Ajustar LabelConverter
    # =================================================================
    print("Celda 6: Dividiendo datos y entrenando el modelo...")

    (training_data, test_data) = news_df.randomSplit([0.8, 0.2], seed=12345)
    
    training_data_count = training_data.count()
    test_data_count = test_data.count()

    if training_data_count == 0 or test_data_count == 0:
        print("ERROR: No hay suficientes datos en el conjunto de entrenamiento o prueba después de la división.")
        print(f"Training count: {training_data_count}, Test count: {test_data_count}")
        spark.stop()
        sys.exit(1)

    print(f"Filas en Training Data: {training_data_count}")
    print(f"Filas en Test Data: {test_data_count}")

    pipeline_model = pipeline.fit(training_data)
    fitted_label_indexer_model = pipeline_model.stages[0] 
    original_labels = fitted_label_indexer_model.labels
    label_converter.setLabels(original_labels)
    print("Modelo entrenado.")

    # Celda 7: Hacer Predicciones y Evaluar el Modelo
    # ==============================================
    print("Celda 7: Haciendo predicciones y evaluando el modelo...")

    predictions_raw = pipeline_model.transform(test_data)
    predictions_df = label_converter.transform(predictions_raw)

    print("Muestra de predicciones:")
    predictions_df.select("TITULAR", "CATEGORIA", "label_index", "prediction", "predicted_CATEGORIA", "probability").show(20, truncate=True)

    evaluator_accuracy = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator_accuracy.evaluate(predictions_df)
    print(f"Accuracy del modelo: {accuracy:.4f}")

    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="f1")
    f1_score = evaluator_f1.evaluate(predictions_df)
    print(f"F1-Score del modelo: {f1_score:.4f}")

    evaluator_precision = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="weightedPrecision")
    precision = evaluator_precision.evaluate(predictions_df)
    print(f"Precision ponderada: {precision:.4f}")

    evaluator_recall = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="weightedRecall")
    recall = evaluator_recall.evaluate(predictions_df)
    print(f"Recall ponderado: {recall:.4f}")
    print("Evaluación completada.")

    # Celda 8: Guardar los Resultados de las Predicciones en S3
    # =======================================================
    print("Celda 8: Guardando resultados de las predicciones en S3...")

    output_predictions_df = predictions_df.select(
        "TITULAR",
        "CATEGORIA",       
        "predicted_CATEGORIA", 
        "probability"      
    )

    try:
        output_predictions_df.write.mode("overwrite").parquet(S3_OUTPUT_PATH)
        print(f"Resultados de las predicciones guardados exitosamente en: {S3_OUTPUT_PATH}")
    except Exception as e:
        print(f"ERROR al guardar los resultados en S3: {e}")
        print("Verifica los permisos de escritura del rol de EMR en el bucket S3 de destino.")
        # No necesariamente terminar el script aquí, la evaluación ya se hizo.

    print("Proceso de Machine Learning completado.")
    spark.stop()
    print("SparkSession detenida.")

if __name__ == "__main__":
    print("Iniciando script de clasificación de noticias...")
    main()
    print("Script finalizado.")