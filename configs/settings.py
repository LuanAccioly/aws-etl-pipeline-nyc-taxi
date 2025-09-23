# --- Configurações Gerais da AWS ---
AWS_REGION = "us-east-1"

# --- Configurações do S3 ---
S3_BUCKET_NAME = "aws-etl-pipeline-nyc-taxi"
BRONZE_S3_PREFIX = "bronze/nyc_taxi"
BRONZE_S3_PATH = f"s3a://{S3_BUCKET_NAME}/bronze/nyc_taxi/"
SILVER_S3_PATH = f"s3a://{S3_BUCKET_NAME}/silver/nyc_taxi_trips/"
GOLD_S3_PATH = f"s3a://{S3_BUCKET_NAME}/gold/daily_trip_summary/"

# --- Configurações do RDS PostgreSQL ---
# O nome do segredo armazenado no AWS Secrets Manager
RDS_SECRET_NAME = "aws-etl-pipeline-nyc-taxi/postgres"

# O nome da tabela de destino no banco de dados
RDS_TABLE_NAME = "silver_nyc_taxi_trips"

# --- Configurações do AWS Glue Data Catalog ---
GLUE_DATABASE_NAME = "nyc_taxi_db"
GLUE_SILVER_TABLE_NAME = "silver_nyc_taxi_trips"
GLUE_GOLD_TABLE_NAME = "daily_trip_summary"
