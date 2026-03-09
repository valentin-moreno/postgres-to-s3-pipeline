import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
from dotenv import load_dotenv
import boto3
import os
import logging

# Configurar logging para registrar el progreso en consola con timestamps
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# Cargar variables de entorno desde el archivo .env (credenciales fuera del código)
load_dotenv()

# =============================================================================
# CONFIGURACIÓN — Postgres
# =============================================================================
PG_HOST     = "localhost"
PG_PORT     = 5432
PG_DATABASE = "database-finalwork"
PG_USER     = "postgres"
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")   # Nunca hardcodear contraseñas

# Crear el engine de SQLAlchemy con pool de conexiones por defecto
engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}",
    pool_pre_ping=True      # Verifica que la conexión esté activa antes de usarla
)

# =============================================================================
# CONFIGURACIÓN — AWS S3
# =============================================================================
S3_BUCKET = "eia-2025-02-finalwork-vmv"
S3_FOLDER = "data-integration"

# Cliente de S3 (toma credenciales de ~/.aws/credentials o variables de entorno)
s3 = boto3.client("s3")

# =============================================================================
# TABLAS A EXPORTAR
# Para agregar más tablas simplemente añadir el nombre a esta lista
# =============================================================================
TABLAS = ["comercial", "empleados"]


def tabla_a_s3_csv(tabla: str) -> None:
    """
    Lee una tabla completa de Postgres y la sube a S3 en formato CSV (UTF-8).

    La ruta en S3 sigue la convención:
        s3://<bucket>/data-integration/<tabla>/<tabla>.csv

    Args:
        tabla: Nombre exacto de la tabla en Postgres.
    """
    log.info(f"Iniciando exportación de la tabla '{tabla}'")

    # --- Extracción ---
    # read_sql_table lee la tabla completa; para tablas grandes considerar chunks
    df = pd.read_sql_table(tabla, engine)
    log.info(f"  Tabla '{tabla}' leída — {len(df):,} filas, {len(df.columns)} columnas")

    # --- Transformación ---
    # Serializar el DataFrame a CSV en memoria (evita escribir archivos temporales en disco)
    buffer = StringIO()
    df.to_csv(buffer, index=False, encoding="utf-8")

    # --- Carga ---
    s3_key = f"{S3_FOLDER}/{tabla}/{tabla}.csv"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="text/csv"      # Metadato útil para herramientas que leen desde S3
    )

    log.info(f"  Subido correctamente → s3://{S3_BUCKET}/{s3_key}")


# =============================================================================
# EJECUCIÓN PRINCIPAL
# =============================================================================
if __name__ == "__main__":
    errores = []

    for tabla in TABLAS:
        try:
            tabla_a_s3_csv(tabla)
        except Exception as e:
            # Registrar el error pero continuar con las demás tablas
            log.error(f"Error al procesar '{tabla}': {e}")
            errores.append(tabla)

    # Resumen final
    exitosas = len(TABLAS) - len(errores)
    log.info(f"Pipeline finalizado — {exitosas}/{len(TABLAS)} tablas exportadas correctamente.")

    if errores:
        log.warning(f"Tablas con error: {errores}")
