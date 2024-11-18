# info de la materia: ST0263 Tópicos Especiales en Telemática, 2024-2

Estudiante(s): 
Sara Maria Cardona Villada, smcardonav@eafit.edu.co. 
Vanessa Velez Restrepo, vavelezr@eafit.edu.co. 
Luisa Maria Polanco, lmpolanco1@eafit.edu.co. 
Santiago Arias, sariash@eafit.edu.co. 
Luis Miguel GIraldo, lmgiraldo@eafit.edu.co

Profesor: Alvaro, @eafit.edu.co

# Trabajo 3: Automatización del proceso de Captura, Ingesta, Procesamiento y Salida de datos accionables para la gestión de datos Covid en Colombia (Arquitectura Batch para big data)

## 1. Breve descripción de la actividad
El proyecto tiene como objetivo automatizar todas las etapas del ciclo de vida de un proceso analítico, desde la captura e ingesta de datos de fuentes diversas (archivos y bases de datos) hasta el procesamiento y salida de datos, utilizando servicios en la nube para realizar tareas ETL, análisis descriptivo, y aprendizaje automático. El sistema desarrollado permite la gestión automatizada de los datos de COVID en Colombia usando GCP (Google Cloud Platform).

### 1.1. Qué aspectos se cumplieron o desarrollaron de la actividad (requerimientos funcionales y no funcionales)
- Configuración del entorno en GCP y creación de buckets en Cloud Storage:
  - **Raw Zone** (datos crudos), **Trusted Zone** (datos procesados), y **Refined Zone** (resultados de análisis).
- Ingesta automática:
  - Implementación de una **Cloud Function** que se activa periódicamente para la ingestión de datos desde fuentes externas.
- Procesamiento ETL:
  - Creación de un **cluster Dataproc** en GCP con un nodo principal y dos trabajadores.
  - Implementación de un script en Spark (`covid_etl.py`) para transformar y procesar los datos, realizando validaciones (edades, datos nulos, etc.) y guardando el resultado en la zona **Trusted**.
- Conexión a la base de datos relacional:
  - Configuración de una base de datos MySQL en GCP para almacenar datos simulados y conectar los procesos ETL.
- Automatización del análisis y pipeline de Spark:
  - Creación de un script (`combine_covid_data.py`) para combinar y analizar datos provenientes de los buckets.
  - Automatización de la ejecución mediante **Cloud Scheduler** y **workflows**.

### 1.2. Qué aspectos NO se cumplieron o desarrollaron de la activiadd (requerimientos funcionales y no funcionales)


## 2. Información general de diseño de alto nivel, arquitectura, patrones, mejores prácticas utilizadas.
- Arquitectura basada en la nube utilizando **GCP**, dividiendo las zonas de almacenamiento según las etapas del ciclo de vida de los datos.
- Uso de **Spark** para el procesamiento ETL, aprovechando **Jupyter** para pruebas previas a la automatización.
- Implementación de **Cloud Functions** y **Cloud Scheduler** para la automatización de procesos periódicos.
- División clara de las zonas de almacenamiento de datos para un manejo efectivo del ciclo ETL.

## 3. Descripción del ambiente de desarrollo y técnico
- **Lenguaje de programación:** Python 3.9
- **Librerías y paquetes:** 
  - Spark 3.1.2
  - Pandas 1.3.3
  - PyMySQL 1.0.2
- **Buckets creados:**
  - `p3_bucket_1` (Raw Zone)
  - `p3_bucket_2` (Trusted Zone)
  - `p3_bucket_3` (Refined Zone)
- **Componentes utilizados:**
  - **Cloud Function** para la ingestión de datos.
  - **Cloud Scheduler** para la automatización periódica.
  - **Dataproc** para procesamiento en Spark.

### Cómo se compila y ejecuta
- Para la **Cloud Function**, se implementa subiendo los archivos `ingest_covid_data.py` y `requirements.txt` y se despliega desde la consola de GCP.
- **Script ETL** (`covid_etl.py`) se sube al bucket `p3_bucket_1` y se ejecuta creando un trabajo en el cluster **Dataproc**.


  
