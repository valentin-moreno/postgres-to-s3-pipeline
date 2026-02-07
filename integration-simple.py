import pandas as pd
from sqlalchemy import create_engine
import boto3
from io import StringIO
from dotenv import load_dotenv
import os

load_dotenv()

# --- Conexión a Postgres ---
host = "localhost"
port = 5432
database = "database-finalwork"
user = "postgres"
password = os.getenv("postgres_password")

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

# --- Conexión a S3 ---
s3 = boto3.client('s3')

bucket_name = "eia-2025-02-finalwork-vmv"
folder_name = "data-integration"

# --- Tablas a subir ---
tablas_a_subir = ["comercial", "empleados"]

# --- Subir cada tabla a su carpeta ---
for tabla in tablas_a_subir:
    print(f"Procesando tabla: {tabla}")

    # Leer la tabla desde Postgres
    df = pd.read_sql_table(tabla, engine)

    # Convertir a CSV en memoria
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False,encoding='utf-8')

    # Subir a una carpeta con el nombre de la tabla
    s3_file_path = f"{folder_name}/{tabla}/{tabla}.csv"
    s3.put_object(Bucket=bucket_name, Key=s3_file_path, Body=csv_buffer.getvalue())

    print(f"Tabla '{tabla}' subida correctamente a S3 en 's3://{bucket_name}/{s3_file_path}'")

print("¡Tablas seleccionadas subidas correctamente!")