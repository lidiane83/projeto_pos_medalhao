import os
import shutil
import sys
import tempfile
from pathlib import Path
import boto3
from dotenv import load_dotenv

# Eleva o limite de recursão para prevenir estouro de pilha no Windows
sys.setrecursionlimit(25000)

RAIZ = Path(__file__).resolve().parent.parent  # Pasta raiz do projeto
load_dotenv(RAIZ / "docker" / ".env")

ok = True

def linha(nome, cond, dica=""):
    global ok
    ok = ok and bool(cond)
    print("OK " if cond else "FALTA", nome, "" if cond else f"-> {dica}")

# 1. Validação do Java
linha("Java", shutil.which("java") or os.environ.get("JAVA_HOME"),
      "instale o JDK 17 e confira o JAVA_HOME")

# 2. Validação do PySpark (pacote Python)
try:
    import pyspark
    linha(f"PySpark {pyspark.__version__}", True)
except ImportError:
    linha("PySpark", False, "pip install -r requirements.txt")

# 3. Validação do MinIO e Buckets S3
try:
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9001",
        aws_access_key_id=os.environ.get("MINIO_USER", "admin"),
        aws_secret_access_key=os.environ.get("MINIO_PASSWORD", "admin12345"),
        region_name="us-east-1",
    )
    nomes = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    linha("MinIO (buckets landing e warehouse)",
          {"landing", "warehouse"} <= set(nomes),
          "rode: python Scripts/criar_buckets.py")
except Exception as e:
    linha("MinIO", False, f"cd docker && docker compose up -d ({type(e).__name__})")

# 4. Validação da Sessão Spark e leitura/escrita Parquet
try:
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (SparkSession.builder
             .master("local[2]")
             .appName("teste")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.sql.execution.arrow.pyspark.enabled", "false")
             .config("spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled", "false")
             .config("spark.ui.enabled", "false")
             .getOrCreate())

    # Cria o DataFrame diretamente via SQL para evitar erros de serialização local
    df = spark.sql("SELECT 1 as id, 'a' as v UNION ALL SELECT 2, 'b'")
    
    destino = os.path.join(tempfile.gettempdir(), "teste_parquet")
    df.write.mode("overwrite").parquet(destino)
    
    n = spark.read.parquet(destino).count()
    spark.stop()

    linha(f"Spark {pyspark.__version__} (grava e lê Parquet)", n == 2,
          "veja a tabela de erros do Spark")
except Exception as e:
    causa = [l for l in str(e).splitlines() if l.strip()][:3]
    linha("Spark", False, " | ".join(causa))

print("\nPostgres + pgvector e Metabase: vamos montar juntos nas próximas etapas.")
print("Tudo pronto." if ok else "Resolva os itens FALTA e rode de novo.")
sys.exit(0 if ok else 1)