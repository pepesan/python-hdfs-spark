# Requiere: docker/01_launch.sh (servicio seaweedfs) y haber ejecutado
# antes docker/00_init.sh (genera las credenciales en
# docker/seaweedfs/s3-config/s3.json — ver README.md).
#
# Servidor S3 (SeaweedFS) como alternativa a HDFS para subir datos —
# API S3 estándar (boto3), a diferencia de 01_connect_hdfs*.py (WebHDFS)
# y 01_connect_hdfs_04_rpc_nativo_pyarrow.py (RPC nativo). Detalle de por
# qué SeaweedFS y no otra opción en CLAUDE.md.
#
# Documentación: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3.html
import json

import boto3

# Las credenciales las genera docker/00_init.sh (aleatorias en cada
# entorno, nunca fijas en un fichero versionado — ver .gitignore)
with open('docker/seaweedfs/s3-config/s3.json') as f:
    credenciales = json.load(f)['identities'][0]['credentials'][0]

# Conexión a S3
# revisar la configuración de docker/compose.yaml (servicio "seaweedfs")
# 8333 es el puerto publicado de la API S3 (mapea 1:1 al puerto interno)
# region_name es obligatorio para boto3 aunque SeaweedFS lo ignore
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:8333',
    aws_access_key_id=credenciales['accessKey'],
    aws_secret_access_key=credenciales['secretKey'],
    region_name='us-east-1',
)

# nos aseguramos de que el bucket exista (idempotente)
bucket = 'prueba'
buckets_existentes = [b['Name'] for b in s3.list_buckets()['Buckets']]
if bucket not in buckets_existentes:
    s3.create_bucket(Bucket=bucket)

# Listado de objetos del bucket
listado = s3.list_objects_v2(Bucket=bucket).get('Contents', [])
print("listado: " + str([obj['Key'] for obj in listado]))
