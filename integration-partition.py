import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO
from dotenv import load_dotenv
import boto3
import os
import logging

# Logging estructurado para rastrear cada partición procesada
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

load_dotenv()

# =============================================================================
# CONFIGURACIÓN — Postgres
# =============================================================================
PG_HOST     = "localhost"
PG_PORT     = 5432
PG_DATABASE = "database-partition"
PG_USER     = "postgres"
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}",
    pool_pre_ping=True
)

# =============================================================================
# CONFIGURACIÓN — AWS S3
# =============================================================================
S3_BUCKET   = "eia-2025-02-finalwork-vmv"
S3_BASE     = "data-integration/sample-sales-data"  # Prefijo raíz de las particiones

s3 = boto3.client("s3")

# =============================================================================
# CONFIGURACIÓN — Particionamiento
# Hive-style partitioning: country=Colombia/archivo.parquet
# Este esquema es compatible nativamente con Athena, Glue, Spark y Redshift Spectrum,
# lo que permite hacer queries filtradas por país sin leer todo el dataset.
# =============================================================================
TABLA            = "sample_sales_data"
COLUMNA_PARTICION = "country"       # Columna por la que se divide el dataset
COMPRESION        = "gzip"          # gzip tiene mejor ratio que snappy para archivos estáticos


def subir_particion(country: str, df_particion: pd.DataFrame) -> None:
    """
    Serializa una partición del DataFrame a Parquet y la sube a S3
    siguiendo la convención Hive: country=<valor>/archivo.parquet

    Args:
        country:       Valor de la partición (nombre del país).
        df_particion:  Subset del DataFrame correspondiente a ese país.
    """
    # Normalizar el nombre: espacios → guiones bajos (evita problemas en rutas S3)
    country_slug = country.strip().replace(" ", "_")

    # Construir la ruta Hive-style que Athena/Glue reconocen automáticamente
    s3_key = f"{S3_BASE}/{COLUMNA_PARTICION}={country_slug}/sample_sales_data_{country_slug}.parquet"

    # Serializar a Parquet en memoria (sin archivos temporales en disco)
    buffer = BytesIO()
    df_particion.to_parquet(buffer, index=False, engine="pyarrow", compression=COMPRESION)

    size_kb = buffer.getbuffer().nbytes / 1024

    # Subir la partición a S3
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    log.info(f"  [{country}] {len(df_particion):,} filas | {size_kb:.1f} KB → s3://{S3_BUCKET}/{s3_key}")


# =============================================================================
# EJECUCIÓN PRINCIPAL
# =============================================================================
if __name__ == "__main__":

    # --- Extracción completa de la tabla ---
    log.info(f"Leyendo tabla '{TABLA}' desde Postgres...")
    df = pd.read_sql_table(TABLA, engine)
    log.info(f"Total: {len(df):,} filas | {df[COLUMNA_PARTICION].nunique()} países únicos")

    paises = sorted(df[COLUMNA_PARTICION].unique())
    log.info(f"Países encontrados: {paises}")

    # --- Particionamiento y carga ---
    errores = []

    for country, grupo in df.groupby(COLUMNA_PARTICION):
        try:
            subir_particion(country, grupo)
        except Exception as e:
            log.error(f"Error en partición '{country}': {e}")
            errores.append(country)

    # --- Resumen ---
    exitosas = len(paises) - len(errores)
    log.info(f"Pipeline finalizado — {exitosas}/{len(paises)} particiones subidas correctamente.")

    if errores:
        log.warning(f"Particiones con error: {errores}")
