import os
import boto3
from decouple import config

# Cargar las credenciales desde el archivo .env
AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY")

# Configurar el cliente S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# Nombre de tu bucket S3
bucket_name = config("S3_BUCKET_NAME")

# Directorio local donde se descargarán los archivos
local_directory = bucket_name


# Función para descargar objetos de S3 con paginación
def download_objects(bucket, local_dir):
    paginator = s3.get_paginator("list_objects_v2")
    operation_parameters = {"Bucket": bucket}

    for page in paginator.paginate(**operation_parameters):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            local_path = os.path.join(local_dir, key)
            if not os.path.exists(local_path):
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                try:
                    s3.download_file(bucket, key, local_path)
                    print(f"Descargado: {key}")
                except Exception as e:
                    print(f"Error al descargar {key}: {str(e)}")
            else:
                print(f"El archivo {key} ya existe en el directorio local.")


# Descargar objetos del bucket
download_objects(bucket_name, local_directory)

print("Descarga completa.")
