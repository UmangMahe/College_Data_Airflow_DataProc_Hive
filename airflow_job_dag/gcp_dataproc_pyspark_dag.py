from datetime import datetime, timedelta
import uuid  # Import UUID for unique batch IDs
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime.now(),
}

# Define the DAG
with DAG(
    dag_id="gcp_dataproc_pyspark_dag",
    default_args=default_args,
    schedule_interval="0 0 * * *",  # Trigger daily at midnight
    catchup=False,
) as dag:

    # Fetch environment variables
    gcs_bucket = Variable.get("gcs_bucket", default_var="airflow-projects-learning")
    gcp_project = Variable.get("gcp_project", default_var="healthy-splice-469717-n5")

    # # Task 1: File Sensor for GCS
    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket=gcs_bucket,
        object=f"airflow-project-2/data/*.csv",
        use_glob=True,
        google_cloud_conn_id="google_cloud_default",  # GCP connection
        timeout=43200,  # Timeout in seconds
        poke_interval=300,  # Time between checks
        mode="poke",  # Blocking mode
    )

    # Task 2: Submit PySpark job to Dataproc Serverless

    REGION = "us-central1"
    CLUSTER_NAME = "airflow-cluster-dataproc"
    CLUSTER_CONFIG = {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
        },
        "software_config": {
            "image_version": "2.3-debian12",
        },
    }

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster_dataproc",
        project_id=gcp_project,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    PYSPARK_JOB = {
        "reference": {"project_id": gcp_project},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{gcs_bucket}/airflow-project-2/spark-job/dataproc_pyspark_job.py",
            "args": [
                f"--gcp_project={gcp_project}",
                f"--gcs_bucket={gcs_bucket}"
            ],
        },
    }

    pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job_on_dataproc",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=gcp_project,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster_dataproc",
        cluster_name=CLUSTER_NAME,
        region=REGION,
        project_id=gcp_project,
        # Ensures cluster deletion runs regardless of previous task success or failure
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task Dependencies
    file_sensor >> create_cluster >> pyspark_job >> delete_cluster
