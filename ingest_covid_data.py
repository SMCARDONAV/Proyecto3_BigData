import functions_framework
from google.cloud import storage
import requests
import json
from datetime import datetime

@functions_framework.http
def ingest_covid_data(request):
    """
    Cloud Function para ingestar datos de COVID-19 desde datos.gov.co
    y almacenarlos en un bucket de GCS
    """
    # Configuración
    BUCKET_NAME = "p3_bucket_1"
    API_URL = "https://www.datos.gov.co/resource/gt2j-8ykr.json"
    
    try:
        # 1. Obtener datos de la API
        print("Iniciando descarga de datos desde la API...")
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
        
        # 2. Preparar los datos para almacenamiento
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 3. Almacenar en Google Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        # Guardar datos crudos
        blob_name = f'raw/covid_data_{timestamp}.json'
        blob = bucket.blob(blob_name)
        blob.upload_from_string(
            json.dumps(data, ensure_ascii=False),
            content_type='application/json'
        )
        
        print(f'Archivo {blob_name} subido exitosamente al bucket {BUCKET_NAME}')
        
        # 4. Crear archivo de control
        control_data = {
            'timestamp': timestamp,
            'records_count': len(data),
            'source': API_URL,
            'status': 'success',
            'filename': blob_name
        }
        
        control_blob = bucket.blob(f'control/ingestion_log_{timestamp}.json')
        control_blob.upload_from_string(
            json.dumps(control_data),
            content_type='application/json'
        )
        
        return {
            'success': True,
            'message': f'Datos ingestados correctamente. Archivo: {blob_name}',
            'records_processed': len(data)
        }
        
    except Exception as e:
        error_message = f'Error en la ingesta: {str(e)}'
        print(error_message)
        
        # Registrar el error
        if 'storage_client' in locals():
            error_log = {
                'timestamp': datetime.now().strftime('%Y%m%d_%H%M%S'),
                'error': error_message,
                'source': API_URL
            }
            
            error_blob = bucket.blob(
                f'error_logs/error_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            )
            error_blob.upload_from_string(
                json.dumps(error_log),
                content_type='application/json'
            )
        
        return {
            'success': False,
            'error': error_message
        }