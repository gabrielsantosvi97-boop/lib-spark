from pyspark.sql import SparkSession
from spark_utils import get_spark_session
import sys
import logging

logger = logging.getLogger("spark_logger")
logging.basicConfig(level=logging.INFO)
account_id = "114743582235"
aws_region = "sa-east-1"

# ...existing code...

def main():
    # Parâmetros para a sessão
    session_name = "Validacao_Acesso_Tabelas_Bronze_Silver"
    account_id = "114743582235"
    aws_region = "sa-east-1"
    app_name = session_name
    environment = "dev"
    resource_config = {
        "spark.driver.memory": "11g",
        "spark.emr.default.executor.memory": "12g",
        "spark.sql.parquet.mergeSchema": "true"
    }
    spark = get_spark_session(
        session_name=session_name,
        account_id=account_id,
        aws_region=aws_region,
        resource_config=resource_config,
        environment=environment,
        app_name=app_name
    )

    print(f"--- Sessão Spark Iniciada ({session_name}, {environment}) ---")
    origem = spark.conf.get("spark.localiza.debug.origem", "Não detectado (Builder ou Default)")
    print(f" Rastreio de Origem: {origem}")
    print(f" Python Executable: {sys.executable}")
    print(f" Python Version: {sys.version}")

    print("--- Auditoria de Configurações Críticas ---")
    for k, v in sorted(spark.sparkContext.getConf().getAll()):
        print(f"🔹 {k} = {v}")

    # Lista com as tabelas que serão validadas
    tabelas_para_consultar = [
        "vc_jv_bronze.agencia_dbo_age_area",
        "vc_jv_silver.agencia"
    ]

    for tabela_base in tabelas_para_consultar:
        print(f"\n{'='*60}")
        print(f" INICIANDO VALIDAÇÃO PARA A TABELA: {tabela_base}")
        print(f"{'='*60}")

        catalogos = [
            ("spark_catalog", f"spark_catalog.{tabela_base}"),
            ("glue_catalog", f"glue_catalog.{tabela_base}"),
            ("sem_catalogo", tabela_base)
        ]

        for nome_catalogo, tabela in catalogos:
            print(f"\n>>> Consultando ({nome_catalogo}): {tabela}")
            
            # Consulta via spark.sql
            try:
                df = spark.sql(f"SELECT * FROM {tabela} LIMIT 10")
                print("\n--- Schema da Tabela (SQL) ---")
                df.printSchema()
                print("--- Amostra de Dados (Top 10, SQL) ---")
                df.show(truncate=False)
                print(f"\n[SUCESSO] Leitura da tabela {tabela} via spark.sql realizada com sucesso.")
            except Exception as e:
                print(f"\n[ERRO] Falha ao acessar a tabela {tabela} via spark.sql:")
                print(str(e))

            # Consulta via DataFrame API
            try:
                # O acesso direto via DataFrame API depende do catálogo e registro de tabela
                df_api = spark.read.table(tabela)
                print("\n--- Schema da Tabela (DataFrame API) ---")
                df_api.printSchema()
                print("--- Amostra de Dados (Top 10, DataFrame API) ---")
                df_api.show(truncate=False)
                print(f"\n[SUCESSO] Leitura da tabela {tabela} via DataFrame API realizada com sucesso.")
            except Exception as e:
                print(f"\n[ERRO] Falha ao acessar a tabela {tabela} via DataFrame API:")
                print(str(e))

    spark.stop()


if __name__ == "__main__":
    main()