import pandas as pd
from sqlalchemy import create_engine
import boto3
from io import BytesIO
from dotenv import load_dotenv
import os

load_dotenv()

# --- Conexión a Postgres ---
host = "localhost"
port = 5432
database = "database-partition"
user = "postgres"
password = os.getenv("postgres_password")

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

# --- Conexión a S3 ---
s3 = boto3.client('s3')

bucket_name = "eia-2025-02-finalwork-vmv"
base_folder = "data-integration/sample-sales-data"

# --- Leer la tabla desde Postgres ---
df = pd.read_sql_table("sample_sales_data", engine)

# --- Subir cada partición (por país) ---
for country, group in df.groupby("country"):
    print(f"Procesando país: {country}")

    # Crear buffer Parquet en memoria (compresión gzip)
    parquet_buffer = BytesIO()
    group.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='gzip')

    # Normalizar nombre del país
    country_name = country.replace(" ", "_")

    # Ruta en formato particionado y nombre del archivo con el país
    s3_file_path = f"{base_folder}/country={country_name}/sample_sales_data_{country_name}.parquet"

    # Subir a S3
    s3.put_object(Bucket=bucket_name, Key=s3_file_path, Body=parquet_buffer.getvalue())

    print(f"   País '{country}' subido correctamente:")
    print(f"   s3://{bucket_name}/{s3_file_path}")

print("\n¡Todas las particiones con nombre del país fueron subidas correctamente!")