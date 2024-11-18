from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql.functions import col

def create_spark_session():
    """Inicializar sesión Spark con configuraciones necesarias para GCP"""
    spark = SparkSession.builder \
        .appName("CovidDataSQLPipelineAnalysis") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.fs.gs.project.id", "tu-proyecto-id") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.history.fs.logDirectory", "gs://p3_bucket_1/logs/") \
        .config("spark.eventLog.dir", "gs://p3_bucket_1/logs/") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_parquet_data(spark, input_path):
    """Leer datos Parquet desde GCS"""
    try:
        print(f"Leyendo datos desde: {input_path}")
        df = spark.read.parquet(input_path)
        print(f"Datos leídos exitosamente. Registros: {df.count()}")
        return df
    except Exception as e:
        print(f"Error al leer los datos desde {input_path}: {str(e)}")
        raise

def run_sparksql_analysis(spark, df, output_path):
    """Realizar análisis descriptivo usando SparkSQL"""
    try:
        print("Creando vista temporal para SparkSQL...")
        df.createOrReplaceTempView("covid_data")
        
        print("Ejecutando consultas SQL...")
        
        # 1. Contar casos por estado
        cases_by_state = spark.sql("""
            SELECT estado, COUNT(*) AS total_cases
            FROM covid_data
            GROUP BY estado
            ORDER BY total_cases DESC
        """)
        cases_by_state.show()

        # 2. Edad promedio por estado
        avg_age_by_state = spark.sql("""
            SELECT estado, AVG(edad) AS avg_age
            FROM covid_data
            WHERE edad IS NOT NULL
            GROUP BY estado
            ORDER BY avg_age DESC
        """)
        avg_age_by_state.show()
        
        # Guardar resultados
        cases_by_state.write.mode("overwrite").parquet(f"{output_path}/cases_by_state/")
        avg_age_by_state.write.mode("overwrite").parquet(f"{output_path}/avg_age_by_state/")

        print("Análisis SQL completado y guardado exitosamente.")
    except Exception as e:
        print(f"Error durante el análisis SQL: {str(e)}")
        raise

def run_pipeline(df, output_path):
    """Implementar un pipeline simple con SparkML"""
    try:
        print("Configurando pipeline de transformación...")

        # Indexar y codificar columna 'estado'
        state_indexer = StringIndexer(inputCol="estado", outputCol="estado_index")
        state_encoder = OneHotEncoder(inputCol="estado_index", outputCol="estado_encoded")

        # Configurar pipeline
        pipeline = Pipeline(stages=[state_indexer, state_encoder])
        
        # Ejecutar pipeline
        model = pipeline.fit(df)
        transformed_df = model.transform(df)

        # Guardar datos transformados
        transformed_df.write.mode("overwrite").parquet(f"{output_path}/transformed_data/")

        print("Pipeline completado y datos transformados guardados exitosamente.")
    except Exception as e:
        print(f"Error durante el pipeline: {str(e)}")
        raise

def main():
    # Inicializar Spark
    spark = create_spark_session()
    
    try:
        # Ruta de entrada y salida
        input_path = "gs://p3_bucket_3/refined/combined_covid_data/"
        output_path = "gs://p3_bucket_3/refined/analysis_results/"
        
        # Leer datos combinados desde el bucket refined
        combined_df = read_parquet_data(spark, input_path)
        
        # Realizar análisis con SparkSQL
        run_sparksql_analysis(spark, combined_df, output_path)
        
        # Ejecutar pipeline de transformación
        run_pipeline(combined_df, output_path)
        
        print("Proceso de análisis y pipeline completado exitosamente.")
    
    except Exception as e:
        print(f"Error en el proceso general: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
