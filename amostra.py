'''
    Código responsável por reduzir os dados principais em uma
    amostra menor, pois devido as limitações financeiras, o seu
    processamento em larga escala depende de serviços altamente 
    qualificados e pagos. Sendo assim, a amostra possui cerca de 
    1 milhão de dados em uma tabela resultante de meses do ano 
    de 2014 até 2022 de Despesas de Favorecidos.
'''

# @title Setup
from google.cloud import bigquery
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

project = 'bigdata-dados-publicos' # Project ID inserted based on the query results selected to explore
location = 'US' # Location inserted based on the query results selected to explore
client = bigquery.Client(project=project, location=location)

dataset = "DS_PORTAL_TRANSPARENCIA"
table = "TB_DESPESAS_FAVORECIDOS"
table_final = "TB_DESPESAS_FAVORECIDOS_DISTRIBUIDA"

def job_config():
    config = bigquery.LoadJobConfig(schema = [
        bigquery.SchemaField("CODIGO_FAVORECIDO", "STRING"),
        bigquery.SchemaField("NOME_FAVORECIDO", "STRING"),
        bigquery.SchemaField("SIGLA_UF", "STRING"),
        bigquery.SchemaField("NOME_MUNICIPIO", "STRING"),
        bigquery.SchemaField("CODIGO_ORGAO_SUPERIOR", "STRING"),
        bigquery.SchemaField("NOME_ORGAO_SUPERIOR", "STRING"),
        bigquery.SchemaField("CODIGO_ORGAO", "STRING"),
        bigquery.SchemaField("NOME_ORGAO", "STRING"),
        bigquery.SchemaField("CODIGO_UNIDADE_GESTORA", "STRING"),
        bigquery.SchemaField("NOME_UNIDADE_GESTORA", "STRING"),
        bigquery.SchemaField("ANO_E_MES_DO_LANCAMENTO", "STRING"),
        bigquery.SchemaField("VALOR_RECEBIDO", "STRING")
    ])
    config.source_format = bigquery.SourceFormat.CSV
    config.autodetect = False
    config.create_disposition = "CREATE_IF_NEEDED"
    config.field_delimiter = ","
    config.skip_leading_rows = 1
    config.write_disposition = "WRITE_APPEND"
    config.allow_jagged_rows = True
    config.allow_quoted_newlines = True
    config.max_bad_records = 100000
    config.ignore_unknown_values = True
    return config

i = date(2014, 1, 1)
while i <= date(2022, 12, 1):
  sql = f'''SELECT * FROM `{project}.{dataset}.{table}` WHERE ANO_E_MES_DO_LANCAMENTO = {i.strftime("'%m/%Y'")} LIMIT 9260'''
  df = client.query(sql).to_dataframe()
  table_id = project + "." + dataset + "." + table_final
  job = client.load_table_from_dataframe(df, table_id, job_config=job_config()) 
  job.result()
  print(f"job to complete {str(i.strftime('%m/%Y'))}")
  i+=relativedelta(months=+1)