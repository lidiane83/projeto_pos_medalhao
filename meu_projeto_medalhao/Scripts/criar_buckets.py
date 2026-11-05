import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Carrega o arquivo .env da pasta docker
load_dotenv("docker/.env")

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",  # Usar a porta da API S3 (9002)
    aws_access_key_id=os.getenv("MINIO_USER", "admin"),
    aws_secret_access_key=os.getenv("MINIO_PASSWORD", "admin12345"),
    region_name="us-east-1",
)

for bucket in ["landing", "warehouse"]:
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Já existe: {bucket}")
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"Criado com sucesso: {bucket}")