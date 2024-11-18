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


  
## 4. Paso a paso del desarrollo del proyecto

En esta sección se documenta el paso a paso realizado para completar el proyecto, desde la configuración del entorno en la nube hasta la implementación de los procesos de ingesta, procesamiento y almacenamiento de los datos. Se incluirán imágenes relevantes que ilustran cada uno de estos pasos.

### 4.1. Configuración del entorno en GCP
1. **Creación de buckets en cloud storage**:
   - Se crearon tres buckets en GCP para manejar los datos en las distintas etapas:
     - `p3_bucket_1` para la zona **Raw** (datos crudos).
     - `p3_bucket_2` para la zona **Trusted** (datos procesados).
     - `p3_bucket_3` para la zona **Refined** (resultados de análisis).
   - ![image](https://github.com/user-attachments/assets/ed895fc0-c9d8-4cdf-a065-60a16fa1a0a7)


### 4.2. Implementación de la Ingesta Automática
1. **Cloud function para ingesta de datos**:
   - Se desarrolló un script (`ingest_covid_data.py`) para realizar la ingestión automática de los datos.
   - Se configuró una **Cloud Function** en GCP que ejecuta el script periódicamente.
   - ![image](https://github.com/user-attachments/assets/1d6953d2-3ddd-4b54-937f-5eac3533b69c)


2. **Configuración de cloud scheduler**:
   - Para ejecutar la **Cloud Function** automáticamente, se configuró un **Cloud Scheduler** con la periodicidad requerida.
   - ![image](https://github.com/user-attachments/assets/55d1af97-a67e-47f7-859b-0af0e6bc5ad7)


### 4.3. Procesamiento ETL en dataproc
1. **Creación del Cluster Dataproc**:
   - Se configuró un **cluster Dataproc** en GCP con un nodo principal y dos nodos de trabajo.
   - El cluster fue configurado con permisos para acceder a los buckets de almacenamiento.
   - ![image](https://github.com/user-attachments/assets/b52eeeab-c345-4f2d-aba6-ae2939e13bf0)


2. **Implementación del script ETL**:
   - Se desarrolló el script (`covid_etl.py`) para realizar transformaciones ETL, como validación de datos nulos y estandarización de formatos.
   - El script fue subido al bucket `p3_bucket_1` y posteriormente se creó un trabajo en el cluster **Dataproc** para su ejecución.
   - ![image](https://github.com/user-attachments/assets/91f959a7-67eb-4206-90e7-7a69098329d5)


### 4.4. Conexión con base de datos MySQL
1. **Creación y Configuración de la Base de Datos**:
   - Se configuró una base de datos **MySQL** en GCP para almacenar datos simulados.
   - Se crearon las tablas `data_covid` y `data_covid_simulate` para almacenar los datos transformados y los datos de prueba.
   - ![image](https://github.com/user-attachments/assets/b0088abb-19e9-4526-960c-7208f8bcff14)


### 4.5. Automatización del análisis con Spark
1. **Pipeline de Spark para Análisis Descriptivo**:
   - Se desarrolló un script (`combine_covid_data.py`) que combina datos de la base de datos y realiza un análisis descriptivo usando **Spark**.
   - Se subió el script al bucket `p3_bucket_3` y se creó un trabajo en el cluster **Dataproc** para su ejecución.
   - ![image](https://github.com/user-attachments/assets/0a2d227b-abcb-4937-9a4e-284fddb263e4)
.

### 4.6. Automatización y actualización periódica
1. **DataProc para Workflows**:
   - Se utilizó **Jobs** para crear un workflow que automatizara la actualización y ejecución de los procesos periódicos.
   - Este workflow permite la recolección, transformación y análisis sin intervención humana.
   - ![image](https://github.com/user-attachments/assets/40b85690-d3c0-4554-aae8-4e422532b43e)


### 4.7. Resultados y Almacenamiento Final
1. **Almacenamiento de resultados en Refined Zone**:
   - Los resultados del análisis y procesamiento fueron almacenados en `p3_bucket_3`.
   - Estos resultados están listos para ser consultados mediante **Athena** o un endpoint mediante **API Gateway** (aún pendiente).
   - 

---
