import os

import boto3
import kagglehub

S3_BUCKET_NAME = "aws-etl-pipeline-nyc-taxi"

S3_KEY_PREFIX = "bronze/nyc_taxi"

print("Baixando o dataset do KaggleHub...")
path = kagglehub.dataset_download("elemento/nyc-yellow-taxi-trip-data")
print(f"Dataset baixado em: {path}")

s3 = boto3.client("s3")

print(
    f"\nIniciando upload para o bucket '{S3_BUCKET_NAME}' com o prefixo '{S3_KEY_PREFIX}'..."
)
for root, _, files in os.walk(path):
    for file in files:
        local_file_path = os.path.join(root, file)

        relative_path = os.path.relpath(local_file_path, path)
        s3_key = f"{S3_KEY_PREFIX}/{relative_path}"

        try:
            s3.upload_file(local_file_path, S3_BUCKET_NAME, s3_key)
            print(f"  [OK] Enviado '{file}' para 's3://{S3_BUCKET_NAME}/{s3_key}'")
        except Exception as e:
            print(f"  [ERRO] Falha ao enviar '{file}': {e}")

print("\nProcesso de upload concluído.")
