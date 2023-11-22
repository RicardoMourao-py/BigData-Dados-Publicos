'''
    Código responsável pela extração dos dados, transformação e 
    carregamento no Datalake (BigQuery - Google Cloud Platform).
    Com isso, foram carregados cerca de 80 milhões de dados em 
    uma tabela resultante de meses do ano de 2014 até 2022 de 
    Despesas de Favorecidos.
'''

import os
import pandas as pd
from google.cloud import bigquery
# para extrair informações de páginas HTML
from bs4 import BeautifulSoup
# para paginas dinamicas
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import zipfile
from unidecode import unidecode

chrome_options = webdriver.ChromeOptions()
# Criando um driver
driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

# Definindo as variáveis
PROJECT = 'bigdata-dados-publicos'
dataset = "DS_PORTAL_TRANSPARENCIA"

def scrapping():
    print("comecei o scrapping")

    # Entrando na Página
    url = 'https://portaldatransparencia.gov.br/download-de-dados/despesas-favorecidos'
    driver.get(url)
    driver.implicitly_wait(10) # tempo de espera de execução

    # Pegando último mês
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    lista_links_meses = list(soup.find(id = "links-meses"))[-1]['value']
    for i in lista_links_meses:  
        value_ultimo_mes = lista_links_meses[i]['value']
        click_ultimo_mes = driver.find_element("css selector", f'option[value="{value_ultimo_mes}"]')   
        click_ultimo_mes.click()
        
        # click para baixar o tipo no ultimo mes
        click_baixar = driver.find_element(By.ID, "btn")
        click_baixar.click()

        # Espera dos 11 arquivos serem baixados
        all_files = filter(os.path.isfile, os.listdir( os.curdir ))
        files_zip = list(filter(lambda f: f.endswith('.zip'), all_files))
        while len(files_zip) < 11:
            all_files = filter(os.path.isfile, os.listdir( os.curdir ))
            files_zip = list(filter(lambda f: f.endswith('.zip'), all_files))
        
    print(os.getcwd())
    print(os.listdir( os.curdir ))

def get_zip():
    try:
        print("cheguei getzip")
        all_files = filter(os.path.isfile, os.listdir( os.curdir ))
        files_zip = list(filter(lambda f: f.endswith('.zip'), all_files))
        print(f"files_zip: {files_zip}")
        for i in files_zip:
            with zipfile.ZipFile(i, 'r') as zip_ref:
                zip_ref.extractall()
        print("terminei")
    except Exception as e:
        print(f"Something went wrong with scrapping, files could not be downloaded")
        print(e)
        raise e

def tratamento(csv_file):
    df = pd.read_csv(csv_file, encoding='latin-1', sep = ';')

    # Tirando acentos nos nomes das colunas e espaços
    for i in list(df.columns):
        new_column = unidecode(i).replace(" ", "_")
        if ")" in new_column:
            new_column = new_column.replace(")", '_')
        if "(" in new_column: 
            new_column = new_column.replace("(", '_')
        if "ç" in new_column:
            new_column = new_column.replace("ç", 'c')
        if "$" in new_column:
            new_column = new_column.replace("$", 'S')
        if "/" in new_column:
            new_column = new_column.replace("/", '_')
        if "-" in new_column:
            new_column = new_column.replace("-", '')
        if "?" in new_column:
            new_column = new_column.replace("?", '')
        df = df.rename(columns={f'{i}': f'{new_column.replace("__","_").replace("_ ", "")}'})

    df.to_csv(csv_file, index=False)

    print("Tratamento incluido", csv_file)

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

def bigquery_load(list_of_infos):
    client = bigquery.Client(project=PROJECT)
    print(os.getcwd())
    print(os.listdir( os.curdir ))
    all_files = filter(os.path.isfile, os.listdir( os.curdir ))
    files = list(filter(lambda f: f.endswith('.csv'), all_files))
    print(f"CSVs found{files}")
    
    for file_infos in list_of_infos:
        file_name = file_infos
        table_name = 'TB_DESPESAS_FAVORECIDOS'
        dataset = 'DS_PORTAL_TRANSPARENCIA'
        print(f"Starting {file_name} with the following infos {file_infos}...")
        try:
            file = str(list(filter(lambda f: f.startswith(file_name), files))[0])
            print(f"Found the correspoding file {file}")
            with open(file, "rb") as source_file:
                print(f"Loading {file} into BigQuery {dataset}.{table_name}")
                table = PROJECT + "." + dataset + "." + table_name
                load_job = client.load_table_from_file(source_file, table, job_config=job_config())
                load_job.result()
        except Exception as e:
            print(f"Something went wrong with dict {file_infos}, {file_name} couldn't be loaded into {dataset}.{table_name}")
            print(e)
            raise e

def call_functions():
    all_files = filter(os.path.isfile, os.listdir( os.curdir ))
    files_csv = list(filter(lambda f: f.endswith('.csv'), all_files))
    bigquery_load(files_csv)
    return "Job complete"