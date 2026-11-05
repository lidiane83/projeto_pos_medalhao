
"""
lake_local.py - PySpark em modo LOCAL (sem cluster, sem container de Spark).

O Spark roda dentro do Python do aluno (local[N]). O Docker sobe só os serviços:
MinIO (object storage), Postgres+pgvector (serving e vetorial) e Metabase (BI).

Variáveis (todas opcionais; os padrões batem com docker/.env):
  LAKE_STORAGE   minio | local
  SPARK_CORES    núcleos do Spark local (padrão 2)
  SPARK_MEMORY   memória do driver (padrão 2g)
  MINIO_ENDPOINT, MINIO_USER, MINIO_PASSWORD
  PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DB
"""

import glob
import os
import shutil
import sys
from pathlib import Path

# ---------------------------------------------------------------
# Configuração do Python utilizado pelo PySpark
# ---------------------------------------------------------------

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

ROOT = Path(os.environ.get("KIT_ROOT", Path(__file__).resolve().parents[1]))
DATA_DIR = ROOT / "data"
LOCAL_ROOT = ROOT / "lakehouse"

STORAGE = os.environ.get("LAKE_STORAGE", "minio")

MINIO_ENDPOINT = os.environ.get(
    "MINIO_ENDPOINT", "http://localhost:9001"
)

MINIO_USER = os.environ.get("MINIO_USER", "admin")
MINIO_PASSWORD = os.environ.get("MINIO_PASSWORD", "admin12345")

PG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", "5432")),
    user=os.environ.get("PG_USER", "aula"),
    password=os.environ.get("PG_PASSWORD", "aula123"),
    dbname=os.environ.get("PG_DB", "serving")
)

# ---------------------------------------------------------------
# Dependências Java do Spark
# ---------------------------------------------------------------

ICEBERG_VERSION = "1.6.1"

PACKAGES = [
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.postgresql:postgresql:42.7.4",
]


# ---------------------------------------------------------------
# Inicialização do Spark
# ---------------------------------------------------------------

def get_spark(
    app_name="aula-medalhao",
    cores=None,
    memory=None,
    shuffle_partitions=4
):
    """
    SparkSession local com catálogo Iceberg lake.
    Namespaces: bronze, silver e gold.
    """

    if not shutil.which("java") and not os.environ.get("JAVA_HOME"):
        raise RuntimeError(
            "Java não encontrado. Instale um JDK 17 (ou 11) "
            "e reinicie o Jupyter."
        )

    from pyspark.sql import SparkSession

    cores = cores or os.environ.get("SPARK_CORES", "2")
    memory = memory or os.environ.get("SPARK_MEMORY", "2g")

    # Configuração da sessão Spark
    b = (
        SparkSession.builder
        .master(f"local[{cores}]")
        .appName(app_name)

        # Python Worker e comunicação local
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")

        # Recursos e execução
        .config("spark.driver.memory", memory)
        .config(
            "spark.sql.shuffle.partitions",
            str(shuffle_partitions)
        )
        .config("spark.default.parallelism", str(cores))

        # Configurações SQL
        .config(
            "spark.sql.session.timeZone",
            "America/Sao_Paulo"
        )
        .config(
            "spark.sql.legacy.timeParserPolicy",
            "CORRECTED"
        )
        .config("spark.ui.showConsoleProgress", "false")

        # Dependências
        .config("spark.jars.packages", ",".join(PACKAGES))

        # Extensão Apache Iceberg
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )

        # Catálogo Lakehouse
        .config(
            "spark.sql.catalog.lake",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.defaultCatalog", "lake")
    )

    # -----------------------------------------------------------
    # Armazenamento MinIO
    # -----------------------------------------------------------

    if STORAGE == "minio":

        b = (
            b.config(
                "spark.sql.catalog.lake.warehouse",
                "s3a://warehouse/lakehouse"
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                MINIO_ENDPOINT
            )
            .config(
                "spark.hadoop.fs.s3a.access.key",
                MINIO_USER
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                MINIO_PASSWORD
            )
            .config(
                "spark.hadoop.fs.s3a.path.style.access",
                "true"
            )
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                "false"
            )
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
        )

    else:

        wh = LOCAL_ROOT / "warehouse"
        wh.mkdir(parents=True, exist_ok=True)

        b = b.config(
            "spark.sql.catalog.lake.warehouse",
            str(wh)
        )

    # Criação da sessão
    spark = b.getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Criação dos namespaces
    for ns in ("bronze", "silver", "gold"):
        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS lake.{ns}"
        )

    return spark


# ---------------------------------------------------------------
# S3 / LANDING
# ---------------------------------------------------------------

def _s3():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        region_name="us-east-1"
    )


def landing_uri(sub):
    """URI da landing zone onde os arquivos crus chegam."""

    return (
        f"s3a://landing/{sub}"
        if STORAGE == "minio"
        else str(LOCAL_ROOT / "landing" / sub)
    )


def upload_to_landing(local_dir, prefix):
    """
    Envia uma pasta para a landing.
    """

    local_dir = str(local_dir)

    files = [
        f for f in glob.glob(
            os.path.join(local_dir, "**", "*"),
            recursive=True
        )
        if os.path.isfile(f)
    ]

    if STORAGE == "minio":

        s3 = _s3()

        for f in files:
            s3.upload_file(
                f,
                "landing",
                f"{prefix}/{os.path.relpath(f, local_dir)}"
            )

    else:

        for f in files:
            dest = (
                LOCAL_ROOT
                / "landing"
                / prefix
                / os.path.relpath(f, local_dir)
            )

            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(f, dest)

    return len(files)


def list_landing(prefix=""):

    if STORAGE == "minio":

        resp = _s3().list_objects_v2(
            Bucket="landing",
            Prefix=prefix
        )

        return [
            (o["Key"], o["Size"])
            for o in resp.get("Contents", [])
        ]

    root = LOCAL_ROOT / "landing"

    return [
        (str(Path(f).relative_to(root)), os.path.getsize(f))
        for f in glob.glob(
            str(root / prefix / "**" / "*"),
            recursive=True
        )
        if os.path.isfile(f)
    ]


def list_warehouse(prefix=""):
    """Objetos físicos do warehouse (Parquet + metadados Iceberg)."""

    if STORAGE == "minio":

        out = []

        for page in _s3().get_paginator(
            "list_objects_v2"
        ).paginate(
            Bucket="warehouse",
            Prefix=prefix
        ):

            out += [
                (o["Key"], o["Size"])
                for o in page.get("Contents", [])
            ]

        return out

    root = LOCAL_ROOT / "warehouse"

    return [
        (str(Path(f).relative_to(root)), os.path.getsize(f))
        for f in glob.glob(
            str(root / prefix / "**" / "*"),
            recursive=True
        )
        if os.path.isfile(f)
    ]


# ---------------------------------------------------------------
# POSTGRES
# ---------------------------------------------------------------

def pg_conn():
    import psycopg2
    return psycopg2.connect(**PG)


def pg_query(sql, params=None):
    """SELECT rápido para conferir resultados (devolve pandas)."""

    import pandas as pd

    c = pg_conn()

    try:
        return pd.read_sql_query(sql, c, params=params)

    finally:
        c.close()


def publish_to_postgres(
    df,
    table,
    schema="gold",
    mode="overwrite"
):
    """
    Grava uma tabela Gold no Postgres.
    """

    url = (
        f"jdbc:postgresql://{PG['host']}:"
        f"{PG['port']}/{PG['dbname']}"
    )

    (
        df.write
        .format("jdbc")
        .option("url", url)
        .option("dbtable", f"{schema}.{table}")
        .option("user", PG["user"])
        .option("password", PG["password"])
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode(mode)
        .save()
    )

    return f"{schema}.{table}"


# ---------------------------------------------------------------
# DIAGNÓSTICO
# ---------------------------------------------------------------

def check_env():
    """Confere o ambiente e diz o que falta."""

    ok = True

    def line(nome, cond, dica=""):
        nonlocal ok

        ok &= bool(cond)

        print(
            ("OK   " if cond else "FALTA"),
            nome,
            "" if cond else f"-> {dica}"
        )

    line(
        "Java",
        shutil.which("java") or os.environ.get("JAVA_HOME"),
        "instale um JDK 17"
    )

    try:
        import pyspark
        line(f"PySpark {pyspark.__version__}", True)

    except ImportError:
        line(
            "PySpark",
            False,
            "pip install -r requirements.txt"
        )

    if STORAGE == "minio":

        try:
            names = [
                b["Name"]
                for b in _s3().list_buckets()["Buckets"]
            ]

            line(
                "MinIO (buckets landing e warehouse)",
                {"landing", "warehouse"} <= set(names),
                "cd docker && docker compose up -d"
            )

        except Exception as e:

            line(
                "MinIO",
                False,
                f"cd docker && docker compose up -d ({type(e).__name__})"
            )

    else:

        line("Armazenamento local (sem MinIO)", True)

    try:
        with pg_conn() as c, c.cursor() as cur:

            cur.execute(
                "SELECT extname FROM pg_extension WHERE extname='vector'"
            )

            line(
                "Postgres + pgvector",
                cur.fetchone() is not None,
                "imagem pgvector/pgvector (docker compose)"
            )

    except Exception as e:

        line(
            "Postgres",
            False,
            f"cd docker && docker compose up -d ({type(e).__name__})"
        )

    print(
        "\nTudo pronto."
        if ok
        else "\nResolva os itens FALTA e rode de novo."
    )

    return ok
