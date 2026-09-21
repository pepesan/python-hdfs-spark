# Requiere: docker/01_launch.sh (servicio seaweedfs) y haber ejecutado
# antes docker/00_init.sh (genera las credenciales en
# docker/seaweedfs/s3-config/s3.json — ver README.md).
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
buckets_existentes = [b['Name'] for b in s3.list_buckets()['Buckets']]
if bucket not in buckets_existentes:
    s3.create_bucket(Bucket=bucket)

# Subida de ficheros
# origen y destino, igual que 01_connect_hdfs_02_ficheros.py con WebHDFS
s3.upload_file('./files/local-file.txt', bucket, 'remote-file.txt')

# Coger listado de objetos
claves = [obj['Key'] for obj in s3.list_objects_v2(Bucket=bucket).get('Contents', [])]
# Contenido del 1º objeto
if len(claves) > 0:
    print("Primer objeto: " + claves[0])
    contenido = s3.get_object(Bucket=bucket, Key=claves[0])['Body'].read()
    print(contenido)

# borramos el objeto
s3.delete_object(Bucket=bucket, Key='remote-file.txt')

# con esto escribimos un objeto directamente (sin fichero local de por medio)
s3.put_object(Bucket=bucket, Key='myfile.txt', Body=b'Hello, world!')
# leemos el contenido
contenido = s3.get_object(Bucket=bucket, Key='myfile.txt')['Body'].read()
print(contenido)

# borramos el objeto
s3.delete_object(Bucket=bucket, Key='myfile.txt')
