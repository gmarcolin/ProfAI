import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException

with DAG(
    dag_id = "test_GM",
    start_date = pendulum.datetime(2025,1,1),
    catchup = False,
    schedule = None
) as dag:

    # task iniziale
    start = EmptyOperator(task_id="start")

    # task caricamento dataset
    @task
    def load_dataset():
        print("Dataset loaded")
        return "path\\folder\\file"

    load_dataset_task = load_dataset()

    # task controllo dataset
    def check_dataset(dataset):
        # ti = kwargs['ti']
        # xcom_value = ti.xcom_pull(task_ids='load_dataset')
        if dataset is None:
            raise AirflowException("ERRORE: Dataset nullo!")

        return 'train_model'
   
    check_dataset_task = BranchPythonOperator(
        task_id='check_dataset',
        python_callable=check_dataset,
        op_kwargs={
            "dataset": load_dataset_task
        }    
    )

    # task errore
    @task

    # task addestramento modello
    @task
    def train_model(dataset_path):
        print(f"Model trained based on {dataset_path}")

    train_model_task = train_model(load_dataset_task)

    # dipendenze
    start >> load_dataset_task >> check_dataset_task >> train_model_task
