# Leer los archivos del bucket3 y consumirlos mediante api-gateway
import functions_framework
from google.cloud import storage
import json

@functions_framework.http
def read_files(request):
    """
    Cloud Function para leer múltiples archivos de un bucket en Google Cloud Storage
    y devolver su contenido en formato JSON.
    """
    # Configuración del bucket y archivos
    BUCKET_NAME = "p3_bucket_3"
    FOLDER_NAME = "refined/ml_predictions"
    FILE_NAMES = [
        "part-00000-c2a9f6e4-37ba-4066-b09d-b62699a1c46c-c000.csv",
        "part-00001-c2a9f6e4-37ba-4066-b09d-b62699a1c46c-c000.csv",
        "part-00002-c2a9f6e4-37ba-4066-b09d-b62699a1c46c-c000.csv",
        "part-00003-c2a9f6e4-37ba-4066-b09d-b62699a1c46c-c000.csv"
    ]

    try:
        # Inicializar el cliente de almacenamiento
        print("Iniciando lectura de archivos desde el bucket...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        combined_data = []

        # Leer los archivos uno por uno
        for file_name in FILE_NAMES:
            file_path = f"{FOLDER_NAME}/{file_name}"
            blob = bucket.blob(file_path)

            if blob.exists():
                # Leer contenido del archivo
                file_content = blob.download_as_text()
                combined_data.append({"file": file_name, "content": file_content})
            else:
                # Archivo no encontrado
                combined_data.append({"file": file_name, "error": "File not found"})

        print("Lectura completada exitosamente.")

        # Formatear respuesta como un diccionario JSON
        return {
            "success": True,
            "message": "Archivos leídos correctamente desde GCS.",
            "data": combined_data
        }

    except Exception as e:
        # Manejar errores
        error_message = f"Error al leer los archivos: {str(e)}"
        print(error_message)
        return {
            "success": False,
            "message": error_message
        }
