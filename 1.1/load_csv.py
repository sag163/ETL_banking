
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook


from utils.operator import ReadCSVOperator
from utils.sql_query import SYS, CREATE_LOG, INSERT_LOG
from datetime import timedelta, datetime


with DAG(
        dag_id='load_csv3',
        start_date=datetime(2021, 5, 30),
        schedule_interval=timedelta(minutes=1),
        catchup=False,
) as dag:

    create_logs_task = PostgresOperator(
        task_id='create_logs_task',
        postgres_conn_id='postgres_default',
        sql=CREATE_LOG)

    insert_logs_task = PostgresOperator(
        task_id='insert_logs_task',
        postgres_conn_id='postgres_default',
        sql=INSERT_LOG,
        params={
                'on_date': datetime.now(),
                'status': 'INFO',
                'message': 'Start load data',
        }
    )

    insert_logs_end_task = PostgresOperator(
        task_id='insert_logs_end_task',
        postgres_conn_id='postgres_default',
        sql=INSERT_LOG,
        params={
                'on_date': datetime.now(),
                'status': 'INFO',
                'message': 'Finish load data',
        }
    )

    create_logs_task >> insert_logs_task

    tasks = []

    for query in SYS:
        create_table = PostgresOperator(
            task_id=f"create_table_task_{query}",
            sql=SYS[query][0],
            postgres_conn_id='postgres_default',
        )

        get_data_task = ReadCSVOperator(
            task_id=f'get_data_{query}',
            filename=query,
            query_name=query,
            dag=dag,
            do_xcom_push=True,
        )

        @task(task_id=f'insert_data_{query}')
        def insert_data_task(query, **kwargs):
            """Print the Airflow context and ds variable from the context."""
            try:

                r_params = kwargs['ti'].xcom_pull(
                    task_ids=f"get_data_{query}", key="return_value")
                params = r_params[0]

                pg_hook = PostgresHook('postgres_default')
                engine = pg_hook.get_sqlalchemy_engine()
                r_query = SYS[query][1]
                for param in params:
                    engine.execute(r_query.replace('QUERY', param))

                engine.execute(r_query.replace('QUERY', param))
            except Exception as error:
                pass

        insert = insert_data_task(query)

        create_table >> get_data_task >> insert >> insert_logs_end_task

        tasks.append(create_table)

    insert_logs_task.set_downstream(tasks)


