from pyspark.sql import SparkSession

def create_spark_session():
    """Inicializar sesión Spark con configuraciones necesarias para GCP"""
    spark = SparkSession.builder \
        .appName("CombineCovidData") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.fs.gs.project.id", "tu-proyecto-id") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.history.fs.logDirectory", "gs://p3_bucket_1/logs/") \
        .config("spark.eventLog.dir", "gs://p3_bucket_1/logs/") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_mysql_table(spark, table_name, mysql_url, mysql_properties):
    """Leer una tabla desde MySQL"""
    try:
        print(f"Leyendo tabla: {table_name}")
        df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)
        print(f"Tabla {table_name} leída exitosamente. Registros: {df.count()}")
        return df
    except Exception as e:
        print(f"Error al leer la tabla {table_name}: {str(e)}")
        raise

def combine_dataframes(df1, df2):
    """Combinar dos DataFrames"""
    try:
        print("Combinando DataFrames...")

        # Asegurarnos de que no haya columnas duplicadas
        # Renombrar la columna en `df1` si ya existe en `df2`
        if "ciudad_municipio_nom" in df1.columns and "ciudad_municipio_nom" in df2.columns:
            df1 = df1.drop("ciudad_municipio_nom")
        
        # Renombrar columna en el primer DataFrame para alinearla con el segundo
        df1_aligned = df1.withColumnRenamed("ciudad_municipio", "ciudad_municipio_nom")

        # Realizar la combinación
        combined_df = df1_aligned.unionByName(df2, allowMissingColumns=True)
        print(f"Combinación completada. Registros combinados: {combined_df.count()}")
        return combined_df
    except Exception as e:
        print(f"Error al combinar los DataFrames: {str(e)}")
        raise

        
def save_data(df, output_path):
    """Guardar datos combinados en formato parquet"""
    try:
        print(f"Guardando datos en: {output_path}")
        df.write \
            .mode("overwrite") \
            .parquet(output_path)
        print("Datos guardados exitosamente")
    except Exception as e:
        print(f"Error al guardar los datos: {str(e)}")
        raise

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

def main():
    # Inicializar Spark
    spark = create_spark_session()
    
    try:
        # Configuración de conexión MySQL
        mysql_url = "jdbc:mysql://35.232.61.186:3306/covid_db"
        mysql_properties = {
            "user": "data",
            "password": "data",
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        
        # Rutas
        bucket_data_path = "gs://p3_bucket_2/trusted/covid_processed/"
        output_path = "gs://p3_bucket_3/refined/combined_covid_data/"
        
        # Leer tabla desde MySQL
        data_covid_simulate = read_mysql_table(spark, "data_covid_simulate", mysql_url, mysql_properties)
        
        # Leer datos desde el bucket
        data_covid_bucket = read_parquet_data(spark, bucket_data_path)
        
        # Combinar tablas
        combined_df = combine_dataframes(data_covid_bucket, data_covid_simulate)
        
        # Guardar resultado en GCS
        save_data(combined_df, output_path)
        
        print("Proceso de combinación completado exitosamente")
    
    except Exception as e:
        print(f"Error en el proceso ETL: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
