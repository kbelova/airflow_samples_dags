import logging
import os

import pendulum
from airflow.macros import random, ds_add, datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

RETRIES_NUM = 2
RETRIES_DELAY = timedelta(minutes=2)
RETRY_EXPONENTIAL_BACKOFF = True

# here we provide an additional paths where to search for sql or any other type of files
# by default system will do a lookup only inside $AIRFLOW_HOME/dag folder, this way we extend it
# useful when you would like to store sql files separately from your dags.
TEMPLATE_SEARCHPATH = [f"{os.environ.get('AIRFLOW_HOME')}/sql", "/opt/airflow/", "/opt/airflow/sql"]

ARGS = {
    "owner": "airflow",
    "provide_context": True,
    "retries": RETRIES_NUM,
    "retry_delay": RETRIES_DELAY,
    "retry_exponential_backoff": RETRY_EXPONENTIAL_BACKOFF,
}

dag = DAG(
    dag_id="schedule_postgres_function",
    description="example pipeline, demonstrates power of xcom messages",
    schedule_interval="*/15 * * * *",
    dagrun_timeout=timedelta(minutes=20),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    template_searchpath=TEMPLATE_SEARCHPATH,
    catchup=False,
)  # run every 15 minutes

# put here connection name to your local or not postgress instance
# the easiest way to setup connection is via webUI
conn_id = "<placeholder>"


def send_message(**context):
    """
    Push random number into xcom
    :param context: dict of dict contains all the information regarding the dag_run
    :return:
    """
    # get task instance from the context
    ti = context["ti"]
    # correct call of function imported from the function
    ti.xcom_push(key="my_unique_message", value=random())


send_xcom = PythonOperator(task_id="send_xcom", provide_context=True, python_callable=send_message, dag=dag)


def print_context(**context):
    """
    Simple function showing an example how to:
      * work with context;
      * receive xcom messages;
      * work with f-Strings in python3;
      * get values from templates_dict;

    :param context: dict of dict containing everything about this dag_run
    :return:
    """

    # Work with the context. Keep in mind context inside the task is READ-ONLY
    ti = context["task_instance"]
    logging.info(f"++++ Everything what is stored at task_instance: {dir(ti)}, {vars(ti)} ")
    logging.info(f"++++ We get from xcom, next message: { ti.xcom_pull(key='my_unique_message') } +++++")
    logging.info(f"""++++ Calculated future date is: { ds_add(datetime.now().strftime("%Y-%m-%d"), 5) } ++++""")
    # f-String usage
    logging.debug(f"++++ Relieve value from the templates dictionary: { context['templates_dict']['something'] } ")

    # Work with Jinja, variable string (return value of the variable evaluation)
    logging.debug(f"!!!! Jinja in string: { context['templates_dict']['jinjia_in_string'] }")
    logging.debug(f"!!!! Jinja in f-String: { context['templates_dict']['jinjia_in_fstring'] }")
    logging.debug(f"!!!! Jinja XCOM in f-String: { context['templates_dict']['jinjia_xcom_in_fstring'] }")

    # When function is returning result it would be also written into xcom and can be discovered by the key `returning_value` + this task_id
    return "Simple example how to schedule SQL execution"


log_context = PythonOperator(
    task_id="log_context",
    provide_context=True,
    python_callable=print_context,
    templates_dict={
        "something": 111,
        "jinjia_in_string": "{{ ds }}",
        "jinjia_in_fstring": f"{{{{ ds }}}}",
        "jinjia_xcom_in_fstring": f"{{{{ ti.xcom_pull(key='my_unique_message', task_ids='{send_xcom.task_id}') }}}}",
    },
    dag=dag,
)


pg_task = PostgresOperator(
    task_id="pg_task",
    sql="sql/example_sql_with_jinjia.sql",
    postgres_conn_id=conn_id,
    autocommit=True,
    retries=0,
    dag=dag,
)


send_xcom >> log_context >> pg_task
