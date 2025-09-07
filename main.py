import duckdb as dbd, pandas as pd, prefect as pfc
from prefect import flow, task



file = 'pg_student_evaluations.csv'
con = dbd.connect(database='dataEng2.duckdb', read_only=False)
query = "SELECT * FROM bronze.csv_data"

def extract_csv(file_path):
    df = pd.read_csv(file_path)
    df.to_parquet('student_eval.parquet', engine='pyarrow')
    return df

def create_schema(schema_name):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    return con

def load_to_duckdb(df, table_name):
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
    return con

def query_duckdb(query):
    result = con.execute(query).fetchdf()
    return result

def query_bronze():
    result = con.execute("SELECT * FROM bronze.csv_data").fetchdf()
    print("\nConsulta na tabela 'bronze.csv_data' realizada com sucesso!")
    return result

def parquet_duckdb():
    # Cria uma tabela 'bronze' diretamente a partir do arquivo Parquet.
    # Esta é uma das funcionalidades mais poderosas do DuckDB!
    sql_create_table = """
    CREATE OR REPLACE TABLE bronze.parquet AS 
    SELECT * FROM read_parquet('student_eval.parquet');
    """

    con.execute(sql_create_table)

    print("\nTabela 'bronze.parquet' criada no DuckDB com sucesso!")

create_schema('bronze')
csv_data = extract_csv(file)
load_to_duckdb(csv_data, 'bronze.csv_data')
parquet_duckdb()

