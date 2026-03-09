import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO
from dotenv import load_dotenv
import boto3
import os
import logging

# Logging con timestamps para rastrear el pipeline en producción
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# Cargar credenciales desde .env — nunca commitear contraseñas al repositorio
load_dotenv()

# =============================================================================
# CONFIGURACIÓN — Postgres
# =============================================================================
PG_HOST     = "localhost"
PG_PORT     = 5432
PG_DATABASE = "database-partition"
PG_USER     = "postgres"
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# pool_pre_ping=True evita errores de conexiones stale en pipelines de larga duración
engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}",
    pool_pre_ping=True
)

# =============================================================================
# CONFIGURACIÓN — AWS S3
# =============================================================================
S3_BUCKET = "eia-2025-02-finalwork-vmv"
S3_FOLDER = "data-integration"

s3 = boto3.client("s3")

# =============================================================================
# TABLAS A EXPORTAR EN FORMATO PARQUET
# Parquet es columnar y comprimido: ocupa ~5-10x menos espacio que CSV
# y es el formato estándar en pipelines modernos (Spark, Athena, Redshift Spectrum)
# =============================================================================
TABLAS = ["pedido"]

# Compresión Snappy: equilibrio óptimo entre velocidad y tamaño
# Alternativas: 'gzip' (mejor compresión) | 'brotli' (más lento, mejor ratio)
COMPRESION = "snappy"


def tabla_a_s3_parquet(tabla: str) -> None:
    """
    Lee una tabla de Postgres y la sube a S3 en formato Parquet comprimido.

    Ruta en S3:
        s3://<bucket>/data-integration/<tabla>/<tabla>.parquet

    Args:
        tabla: Nombre exacto de la tabla en Postgres.
    """
    log.info(f"Iniciando exportación Parquet de '{tabla}'")

    # --- Extracción ---
    df = pd.read_sql_table(tabla, engine)
    log.info(f"  '{tabla}' leída — {len(df):,} filas | columnas: {list(df.columns)}")

    # --- Transformación ---
    # Serializar a Parquet en memoria usando PyArrow (más rápido que fastparquet)
    # BytesIO actúa como un archivo virtual sin tocar el disco
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", compression=COMPRESION)
    buffer.seek(0)      # Rebobinar el puntero al inicio antes de leer el contenido

    # Tamaño del archivo generado (útil para monitoreo)
    size_kb = buffer.getbuffer().nbytes / 1024
    log.info(f"  Parquet generado en memoria — {size_kb:.1f} KB (compresión: {COMPRESION})")

    # --- Carga ---
    s3_key = f"{S3_FOLDER}/{tabla}/{tabla}.parquet"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    log.info(f"  Subido correctamente → s3://{S3_BUCKET}/{s3_key}")


# =============================================================================
# EJECUCIÓN PRINCIPAL
# =============================================================================
if __name__ == "__main__":
    errores = []

    for tabla in TABLAS:
        try:
            tabla_a_s3_parquet(tabla)
        except Exception as e:
            log.error(f"Error al procesar '{tabla}': {e}")
            errores.append(tabla)

    exitosas = len(TABLAS) - len(errores)
    log.info(f"Pipeline finalizado — {exitosas}/{len(TABLAS)} tablas exportadas en Parquet.")

    if errores:
        log.warning(f"Tablas con error: {errores}")
