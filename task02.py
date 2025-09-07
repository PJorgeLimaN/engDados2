import duckdb as dbd, pandas as pd, prefect as pfc
from prefect import flow, task

bronze_query = "SELECT * FROM bronze.csv_data"

def create_schema(schema_name, con):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

@task(name="fetch_bronce_data", retries=3, retry_delay_seconds=5)
def fetch_bronze_data(con) -> pd.DataFrame:
    result = con.execute(bronze_query).df()
    print("\nConsulta na tabela 'bronze.csv_data' realizada com sucesso!")
    return result

@task(name="create_silver_eval_table", retries=3, retry_delay_seconds=5)
def create_silver_eval_table(df: pd.DataFrame, con):
    create_schema('silver', con)
    con.execute("CREATE TABLE IF NOT EXISTS silver.student_eval AS SELECT * FROM df")
    print("\nTabela 'silver.student_eval' criada com sucesso!")

@flow(name="silver_flow", retries=3, retry_delay_seconds=5, log_prints=True)
def silver_flow():
    con = dbd.connect(database='dataEng2.duckdb', read_only=False)
    df_bronze = fetch_bronze_data(con)
    create_silver_eval_table(df_bronze, con)
    con.close()

if __name__ == "__main__":
    silver_flow.serve(
        name="main_silver_flow",
        cron="0 0 * * *", 
    )