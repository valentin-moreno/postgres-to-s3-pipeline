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
folder_name = "data-integration"

# --- Tablas a subir ---
tablas_a_subir = ["pedido"]

# --- Subir cada tabla a su carpeta en formato Parquet ---
for tabla in tablas_a_subir:
    print(f"Procesando tabla: {tabla}")

    # Leer la tabla desde Postgres
    df = pd.read_sql_table(tabla, engine)

    # Convertir DataFrame a Parquet en memoria
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')

    # Definir la ruta en S3
    s3_file_path = f"{folder_name}/{tabla}/{tabla}.parquet"

    # Subir a S3
    s3.put_object(Bucket=bucket_name, Key=s3_file_path, Body=parquet_buffer.getvalue())

    print(f" Tabla '{tabla}' subida correctamente a S3 en formato Parquet:")
    print(f"   s3://{bucket_name}/{s3_file_path}")

print("\n ¡Todas las tablas seleccionadas fueron subidas correctamente a S3 en formato Parquet!")