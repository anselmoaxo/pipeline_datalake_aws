from pyspark.sql import SparkSession  
from pyspark.sql.functions import col, unix_timestamp, when  
from pyspark.sql.types import TimestampType 
import argparse  

# Função principal que faz a transformação de dados
# database: nome do banco de dados Hive
# table_source: tabela de origem
# table_target: tabela de destino (a ser definida mais tarde)
def transform_data(database: str, table_source: str, table_target: str) -> None:
    # Cria uma SparkSession com suporte a Hive
    spark = (SparkSession.builder
             .appName("Boston 311 Service Requests Analysis")  # Nome da aplicação
             .enableHiveSupport()  # Habilita suporte ao Hive
             .getOrCreate())  # Cria a sessão Spark
    
    # Lê a tabela de origem do banco de dados Hive
    df = spark.read.table(f"`{database}`.`{table_source}`")
    
    # Converte as colunas "open_dt", "closed_dt" e "target_dt" para o tipo Timestamp
    df = (df.withColumn("open_dt", col("open_dt").cast(TimestampType()))  # Converte open_dt para Timestamp
            .withColumn("closed_dt", col("closed_dt").cast(TimestampType()))  # Converte closed_dt para Timestamp
            .withColumn("target_dt", col("target_dt").cast(TimestampType()))  # Converte target_dt para Timestamp
            )
    
    # Cria uma nova coluna "delay_days" que calcula o atraso em dias
    # Se "closed_dt" for maior que "target_dt", calcula a diferença em segundos e converte para dias
    df = df.withColumn("delay_days", when(
                col("closed_dt") > col("target_dt"),
                (unix_timestamp("closed_dt") - unix_timestamp("target_dt")) / 86400,  # Diferença em dias
                ).otherwise(0)  # Caso contrário, define o atraso como 0
                )
                        
    # Lista de colunas que serão mantidas após a transformação
    columns_to_keep = [
        "case_enquiry_id",  # ID do caso de solicitação
        "open_dt",  # Data de abertura
        "closed_dt",  # Data de fechamento
        "target_dt",  # Data alvo de resolução
        "case_status",  # Status do caso
        "ontime",  # Se o caso foi resolvido no prazo
        "closure_reason",  # Motivo do fechamento
        "case_title",  # Título do caso
        "subject",  # Assunto do caso
        "reason",  # Razão do caso
        "neighborhood",  # Bairro
        "location_street_name",  # Nome da rua
        "location_zipcode",  # Código postal
        "latitude",  # Latitude
        "longitude",  # Longitude
        "source",  # Fonte do caso
        "delay_days",  # Dias de atraso calculados
    ]
    
    # Seleciona apenas as colunas que são relevantes para o próximo processamento
    df_selected = df.select(columns_to_keep)
