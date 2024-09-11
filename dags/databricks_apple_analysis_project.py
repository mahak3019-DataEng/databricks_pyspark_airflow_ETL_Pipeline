from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

default_args = {
    'owner' : 'mahak-databricks',
    'retries' : 2,
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    dag_id = 'databricks_dag_apple_analysis',
    default_args = default_args,
    start_date = datetime(2024,9,10),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    databricks_apple_analysis_notebook = DatabricksSubmitRunOperator(
        task_id = 'run_apple_analysis_notebook',
        databricks_conn_id = 'databricks_default',
        json = {
            'notebook_path': '/Users/mahakajju30@gmail.com/Apple_Data_Analysis/Apple_Analysis1',
            'new_cluster': {
                'spark_version': '12.2.x-scala2.12',  # Adjust Spark version as needed
                'node_type_id': 'dev-tier-node',  # Adjust node type as needed
                'num_workers': 0  # Adjust number of workers as needed
            }
        }
    )
    databricks_apple_analysis_notebook

