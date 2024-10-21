from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

# Funcao que executa o pipeline
def run_pipeline():
    # Carregando o arquivo CSV dentro do container do docker
    dados = '/opt/airflow/dados/df_fraud_credit.csv' 
    df = pd.read_csv(dados)

    monitorar_qualidade_dados(df)

# Funcao para data quality
def monitorar_qualidade_dados(df):
    # Conversao da coluna amount para numerico substituindo valores invalidos por nan
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['risk_score'] = pd.to_numeric(df['risk_score'], errors='coerce')

    # Informacoes sobre o dataframe
    print("Informacoes sobre o dataframe:")
    df.info()

    # Verificando a quantidade de registros e colunas
    total_registros = len(df)
    total_colunas = len(df.columns)

    print(f"Total de registros: {total_registros}")
    print(f"Total de colunas: {total_colunas}")

    # Valores faltantes
    valores_faltantes = df.isnull().sum()
    total_valores_faltantes = valores_faltantes.sum()
    percentual_faltante = (total_valores_faltantes / (total_registros * total_colunas)) * 100
    print("\nValores Faltantes por Coluna:")
    print(valores_faltantes[valores_faltantes > 0])
    print(f"Total de valores faltantes: {total_valores_faltantes} ({percentual_faltante:.2f}%)")

    # Verificar valores inconsistentes e incorretos
    valores_inconsistentes = df['location_region'].isin(['0', 'none']).sum()
    if valores_inconsistentes > 0:
        print(f"\nValores inconsistentes encontrados na coluna 'location_region': {valores_inconsistentes}")

    # # Verificacao de valores errados na coluna risk_score (caso o range seja entre 0 e 100)
    valores_incorretos = df[~df['risk_score'].between(0, 100, inclusive="both")]['risk_score'].count()
    if valores_incorretos > 0:
        print(f"Valores incorretos encontrados na coluna 'risk_score': {valores_incorretos}")

    # Percentual de conformidade 
    total_erros = total_valores_faltantes + valores_inconsistentes
    percentual_conformidade = ((total_registros * total_colunas - total_erros) / (total_registros * total_colunas)) * 100

    print(f"\nTotal de erros: {total_erros}")
    print(f"Percentual de conformidade: {percentual_conformidade:.2f}%")

    # Informacaoes sobre o dataframe
    print("Informacoes sobre o dataframe:")
    df.info()

    # Limpeza de dados removendo linhas location_region = 0
    df_limpo = df[df['location_region'] != '0']

    # Substituir valores 'none' na coluna risk_score para NaN
    df_limpo['risk_score'] = df_limpo['risk_score'].replace('none', pd.NA)

    # Convertendo coluna risk_score para numerico com 2 casas decimais
    df_limpo['risk_score'] = pd.to_numeric(df_limpo['risk_score'], errors='coerce').round(2)

    # DataFrame apos os tratamentos
    print("\nDados apos tratamento:")
    print(df_limpo.head())

    # Verificando se os valores foram removidos
    print("\nVerificacao da coluna 'location_region':")
    print(df_limpo['location_region'].value_counts())

    print("\nVerificacao da coluna 'risk_score':")
    print(df_limpo['risk_score'].value_counts())

    # Media de risk_score por location_region
    resultado1 = df_limpo.groupby('location_region')['risk_score'].mean().sort_values(ascending=False)

    # Filtrando transacoes do tipo sale e dropando valores ausentes da coluna amount
    df_sales = df_limpo.dropna(subset=['amount']).query("transaction_type == 'sale'")

    # Trocando valores 'none' na coluna amount por NaN
    df_sales['amount'] = df_sales['amount'].replace('none', pd.NA)

    # Convertendo a coluna amount para numerico
    df_sales['amount'] = pd.to_numeric(df_sales['amount'], errors='coerce')

    # Modificando a coluna timestamp para formato datetime
    df_sales['timestamp'] = pd.to_datetime(df_sales['timestamp'], unit='s', errors='coerce')

    # Filtrando a transacao mais recente para cada receiving_address
    data_recent_sales = df_sales.sort_values(by='timestamp', ascending=False).drop_duplicates(subset='receiving_address', keep='first')

    # 3 maiores transacoes por amount
    resultado2 = data_recent_sales.nlargest(3, 'amount')[['receiving_address', 'amount', 'timestamp']]

    # Ajuste de data
    resultado2['timestamp'] = resultado2['timestamp'].dt.date

    # Resultados
    print("Resultado 1 - Media do Risk Score por Regiao:")
    print(resultado1)

    print("\nResultado 2 - 3 Maiores Valores de Amount:")
    print(resultado2)


# Argumentos padrao para a DAG, e definindo dia de comecar a execucao
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),  
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definindo a DAG para executar todo dia(diariamente)
dag = DAG(
    'dag_localiza_',
    default_args=default_args,
    description='Pipeline de processamento de dados',
    schedule_interval=timedelta(days=1),  
)

# Definindo a tarefa
run_pipeline_task = PythonOperator(
    task_id='run_pipeline',
    python_callable=run_pipeline,
    dag=dag,
)

# executando
run_pipeline_task
