from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from datetime import datetime, timedelta


### glue job specific variables
glue_job_name = "tracer-job"
glue_iam_role = "admin_role"
region_name = "ap-south-1"


default_args = {
    'owner': 'me',
    'start_date': datetime(2023, 8, 2),
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}


with DAG(dag_id = 'glue_af_pipeline', default_args = default_args, schedule_interval = None) as dag:
    
    glue_job_step = AwsGlueJobOperator(
            task_id = "glue_job_step",
            job_name = glue_job_name,
            job_desc = f"triggering glue job {glue_job_name}",
            region_name = region_name,
            iam_role_name = glue_iam_role,
            num_of_dpus = 5,
            dag = dag
            )
