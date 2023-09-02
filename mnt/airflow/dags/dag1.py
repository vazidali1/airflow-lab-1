# from datetime import datetime
# from datetime import timedelta

# from airflow import DAG
# from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


# glue_dag = DAG(

# )
# with DAG(
#     dag_id="Python_lambda_dag",
#     description=__doc__,
#     schedule_interval=None,
#     start_date=datetime(2023, 4, 12),
#     tags=["python-spike"],
#     dagrun_timeout=timedelta(minutes=60),
#     catchup=False,
# ) as dag:

#     # Task to run the Glue Job
#     run_glue_job = GlueJobOperator(
#         task_id="run_glue_job",
#         job_name="glue-job_task",
#         region_name= "us-east-1",
#         script_location="s3://tracer-fivetran-dev/poc_output/scripts/airflow_glue_test.py",
#         s3_bucket="tracer-fivetran-dev-demo",
#         iam_role_name="AWSGlueServiceRoleDev",
#         aws_conn_id="aws_default",
#         create_job_kwargs={"GlueVersion": "3.0",
#                            "WorkerType": "G.1X",
#                            "NumberOfWorkers": 4,},      
#     )

#     run_glue_job