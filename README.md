# 🔄 Pipeline de Integración de Datos — Postgres → S3

Scripts de integración de datos que extraen tablas de **PostgreSQL** y las cargan en **AWS S3** en tres modalidades distintas: CSV simple, Parquet comprimido y Parquet particionado por país (Hive-style).

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python) ![Postgres](https://img.shields.io/badge/PostgreSQL-15+-316192?logo=postgresql) ![AWS](https://img.shields.io/badge/AWS_S3-FF9900?logo=amazonaws) ![Parquet](https://img.shields.io/badge/Parquet-Apache-blue)

---

## 📁 Estructura del proyecto

```
data-integration/
├── integration-simple.py              # Postgres → S3 en CSV
├── integration-postgres-parquet-s3.py # Postgres → S3 en Parquet (Snappy)
├── integration-partition.py           # Postgres → S3 particionado por país (Hive-style)
├── .env                               # Credenciales locales (no commitear)
├── requirements.txt
└── README.md
```

---

## ⚙️ Scripts

### 1. `integration-simple.py` — CSV básico
Lee tablas de Postgres y las sube a S3 como archivos CSV UTF-8. Útil para compatibilidad máxima con herramientas que no soportan Parquet.

```
s3://<bucket>/data-integration/<tabla>/<tabla>.csv
```

### 2. `integration-postgres-parquet-s3.py` — Parquet comprimido
Exporta tablas a formato Parquet con compresión Snappy. Reduce el tamaño ~5-10x respecto a CSV y es compatible con Athena, Redshift Spectrum y Spark.

```
s3://<bucket>/data-integration/<tabla>/<tabla>.parquet
```

### 3. `integration-partition.py` — Parquet particionado (Hive-style)
Particiona el dataset por la columna `country` y sube cada partición en su propia carpeta siguiendo la convención Hive. Permite a Athena y Glue hacer **partition pruning** (solo lee las particiones necesarias en cada query).

```
s3://<bucket>/data-integration/sample-sales-data/country=Colombia/sample_sales_data_Colombia.parquet
s3://<bucket>/data-integration/sample-sales-data/country=Mexico/sample_sales_data_Mexico.parquet
...
```

---

## 🚀 Configuración y uso

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Crear el archivo `.env`

```env
POSTGRES_PASSWORD=tu_contraseña_aqui
```

> ⚠️ Agrega `.env` a tu `.gitignore`. Nunca subas credenciales al repositorio.

### 3. Configurar credenciales de AWS

```bash
aws configure
# O mediante variables de entorno:
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
```

### 4. Ejecutar el script deseado

```bash
python integration-simple.py
python integration-postgres-parquet-s3.py
python integration-partition.py
```

---

## 🗂️ Rutas en S3

| Script | Formato | Ruta |
|--------|---------|------|
| `integration-simple.py` | CSV | `data-integration/<tabla>/<tabla>.csv` |
| `integration-postgres-parquet-s3.py` | Parquet (Snappy) | `data-integration/<tabla>/<tabla>.parquet` |
| `integration-partition.py` | Parquet (gzip) | `data-integration/sample-sales-data/country=<país>/...parquet` |

---

## 🛠️ Stack tecnológico

| Herramienta | Uso |
|-------------|-----|
| **PostgreSQL** | Base de datos fuente |
| **SQLAlchemy** | Conexión y lectura de tablas |
| **Pandas** | Manipulación del DataFrame |
| **PyArrow** | Serialización a Parquet |
| **Boto3** | SDK de AWS para subir a S3 |
| **python-dotenv** | Gestión de variables de entorno |

---

## 💡 Decisiones técnicas

- **Parquet sobre CSV**: columnar, comprimido y ~10x más rápido en queries analíticas sobre Athena.
- **Hive-style partitioning**: permite a Athena/Glue/Spark hacer *partition pruning*, reduciendo el costo y tiempo de las queries al evitar escanear particiones innecesarias.
- **BytesIO / StringIO**: serialización en memoria sin archivos temporales en disco, más limpio en entornos efímeros (Lambda, containers).
- **pool_pre_ping=True**: evita errores de conexiones stale en pipelines de larga duración.
- **Logging estructurado**: cada paso registra timestamps, conteo de filas y tamaño del archivo generado.

---

## 📄 Licencia

MIT — libre para uso personal y comercial.
