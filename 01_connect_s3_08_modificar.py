# Requiere: docker/01_launch.sh (servicio seaweedfs) y haber ejecutado
# antes docker/00_init.sh (genera las credenciales en
# docker/seaweedfs/s3-config/s3.json — ver README.md).
#
# Ciclo completo de modificación: sube un fichero a S3, lo descarga, lo
# modifica en memoria, y vuelve a subir la versión modificada
# SOBRESCRIBIENDO el mismo objeto (misma clave) — a diferencia de
# 01_connect_s3_06_ficheros.py, que sube/lee/borra y escribe un objeto
# nuevo aparte, aquí el punto es que el objeto final en S3 es una versión
# transformada del original, no uno distinto.
#
# Documentación: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3.html
import json

import boto3

with open('docker/seaweedfs/s3-config/s3.json') as f:
    credenciales = json.load(f)['identities'][0]['credentials'][0]

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:8333',
    aws_access_key_id=credenciales['accessKey'],
    aws_secret_access_key=credenciales['secretKey'],
    region_name='us-east-1',
)

bucket = 'prueba'
clave = 'fichero-a-modificar.txt'
buckets_existentes = [b['Name'] for b in s3.list_buckets()['Buckets']]
if bucket not in buckets_existentes:
    s3.create_bucket(Bucket=bucket)

# 1) Subimos el fichero original a S3
s3.upload_file('./files/local-file.txt', bucket, clave)

# 2) Lo descargamos (get_object trae el contenido directamente a memoria,
# sin pasar por un fichero local intermedio — a diferencia de
# download_file(), que sí escribe a disco)
original = s3.get_object(Bucket=bucket, Key=clave)['Body'].read()
print("Contenido original:")
print(original)

# 3) Lo modificamos en memoria — aquí, pasando el texto a mayúsculas y
# añadiendo una línea nueva al final. En un caso real esta "modificación"
# podría ser cualquier transformación: limpiar datos, añadir una
# cabecera, convertir el formato...
modificado = original.decode('utf-8').upper() + '\n--- MODIFICADO ---\n'
modificado = modificado.encode('utf-8')

# 4) Volvemos a subir el contenido modificado, con la MISMA clave que el
# original — put_object sobrescribe el objeto existente sin más (S3/
# SeaweedFS no tienen "edición parcial" de un objeto: subir con la misma
# clave siempre reemplaza el objeto entero, no hay forma de cambiar solo
# una parte sin volver a mandar el contenido completo).
s3.put_object(Bucket=bucket, Key=clave, Body=modificado)

# Comprobamos que lo que hay ahora en S3 es la versión modificada, no la
# original
releido = s3.get_object(Bucket=bucket, Key=clave)['Body'].read()
print("Contenido tras sobrescribir con la versión modificada:")
print(releido)
assert releido == modificado
assert releido != original

# limpiamos
s3.delete_object(Bucket=bucket, Key=clave)
