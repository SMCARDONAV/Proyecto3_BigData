from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, lit, current_timestamp,
    year, month, dayofmonth, upper, trim
)
from pyspark.sql.types import IntegerType

def create_spark_session():
    """Inicializar sesión Spark con configuraciones necesarias para GCP"""
    spark = SparkSession.builder \
        .appName("COVID_Colombia_ETL") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.fs.gs.project.id", "tu-proyecto-id") \
        .config("spark.sql.broadcastTimeout", "600") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.history.fs.logDirectory", "gs://p3_bucket_1/logs/") \
        .config("spark.eventLog.dir", "gs://p3_bucket_1/logs/") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_raw_data(spark, input_path):
    """Leer datos JSON desde el bucket raw"""
    try:
        print(f"Intentando leer datos desde: {input_path}")
        df = spark.read \
            .option("multiline", "true") \
            .option("encoding", "UTF-8") \
            .json(input_path)
        print(f"Datos leídos exitosamente. Número de registros: {df.count()}")
        return df
    except Exception as e:
        print(f"Error al leer los datos desde {input_path}: {str(e)}")
        raise

def transform_data(df):
    """Aplicar transformaciones a los datos"""
    try:
        return df \
            .withColumn("fecha_reporte_web", to_timestamp(col("fecha_reporte_web"))) \
            .withColumn("fecha_de_notificaci_n", to_timestamp(col("fecha_de_notificaci_n"))) \
            .withColumn("fecha_diagnostico", to_timestamp(col("fecha_diagnostico"))) \
            .withColumn("fecha_recuperado", to_timestamp(col("fecha_recuperado"))) \
            .withColumn("fecha_muerte", to_timestamp(col("fecha_muerte"))) \
            .withColumn("edad", col("edad").cast(IntegerType())) \
            .withColumn("departamento_nom", upper(trim(col("departamento_nom")))) \
            .withColumn("ciudad_municipio_nom", upper(trim(col("ciudad_municipio_nom")))) \
            .withColumn("estado", when(col("estado").isNull(), "SIN INFORMACIÓN")
                                .otherwise(upper(trim(col("estado"))))) \
            .withColumn("proceso_etl_timestamp", current_timestamp()) \
            .withColumn("año", year(col("fecha_reporte_web"))) \
            .withColumn("mes", month(col("fecha_reporte_web"))) \
            .withColumn("dia", dayofmonth(col("fecha_reporte_web")))
    except Exception as e:
        print(f"Error en las transformaciones: {str(e)}")
        raise

def apply_quality_checks(df):
    """Aplicar validaciones de calidad a los datos"""
    try:
        # Filtrar registros con fechas válidas
        df_validated = df.filter(
            (col("fecha_reporte_web").isNotNull()) & 
            (col("edad").isNotNull() & (col("edad") >= 0) & (col("edad") <= 120))
        )
        
        # Contar registros eliminados
        total_records = df.count()
        valid_records = df_validated.count()
        print(f"Registros totales: {total_records}")
        print(f"Registros válidos: {valid_records}")
        print(f"Registros filtrados: {total_records - valid_records}")
        
        return df_validated
    except Exception as e:
        print(f"Error en las validaciones: {str(e)}")
        raise

def save_processed_data(df, output_path):
    """Guardar datos procesados en formato parquet"""
    try:
        print(f"Guardando datos en: {output_path}")
        df.write \
            .mode("overwrite") \
            .partitionBy("año", "mes") \
            .parquet(output_path)
        print("Datos guardados exitosamente")
    except Exception as e:
        print(f"Error al guardar los datos: {str(e)}")
        raise

def read_data_from_mysql(spark, jdbc_url, table_name, properties):
    """Leer datos desde MySQL usando JDBC"""
    try:
        df = spark.read.jdbc(jdbc_url, table_name, properties=properties)
        print(f"Datos leídos desde MySQL: {df.count()} registros")
        return df
    except Exception as e:
        print(f"Error al leer los datos desde MySQL: {str(e)}")
        raise

def save_data_to_mysql(df, jdbc_url, table_name, properties):
    """Guardar datos transformados en MySQL usando JDBC"""
    try:
        df.write.jdbc(jdbc_url, table_name, mode="append", properties=properties)
        print("Datos guardados en MySQL")
    except Exception as e:
        print(f"Error al guardar los datos en MySQL: {str(e)}")
        raise

def main():
    # Inicializar Spark
    spark = create_spark_session()
    
    # Parámetros de conexión JDBC a MySQL
    jdbc_url = "jdbc:mysql://35.232.61.186:3306/covid"
    properties = {
        "user": "data",
        "password": "data",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    try:
        # Definir rutas
        input_path = "gs://p3_bucket_1/raw/*.json"
        output_path = "gs://p3_bucket_2/trusted/covid_processed"
        mysql_table_name = "data_covid"
        
        print(f"Iniciando proceso ETL...")
        print(f"Ruta de entrada: {input_path}")
        print(f"Ruta de salida: {output_path}")
        
        # Leer datos desde Cloud Storage
        raw_df = read_raw_data(spark, input_path)
        
        if raw_df.rdd.isEmpty():
            raise Exception("No se encontraron archivos JSON en la ruta de entrada")
        
        # Aplicar transformaciones
        transformed_df = transform_data(raw_df)
        
        # Aplicar validaciones de calidad
        validated_df = apply_quality_checks(transformed_df)
        
        # Guardar los datos procesados en Cloud Storage
        save_processed_data(validated_df, output_path)
        
        # Leer los datos desde MySQL
        mysql_df = read_data_from_mysql(spark, jdbc_url, mysql_table_name, properties)
        
        # Transformar los datos de MySQL (si es necesario) y guardarlos en MySQL
        # Si necesitas realizar transformaciones adicionales, hazlo aquí
        save_data_to_mysql(validated_df, jdbc_url, mysql_table_name, properties)
        
        print("Proceso ETL completado exitosamente")
        
    except Exception as e:
        print(f"Error en el proceso ETL: {str(e)}")
        raise e
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
