import logging
import os
from datetime import timedelta, datetime

import pandas
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.models import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

# Cria connection para o operador postgres, esta parte não cabe no tutorial 02
from airflow import settings
from airflow.models import Connection

conn = Connection(
    conn_id='postgres_conn',
    conn_type='Postgres',
    host='postgres',
    login='airflow',
    password='airflow',
    port=5432
)
session = settings.Session()  # get the session
session.add(conn)
session.commit()


def detect_new_student(path_to_students, path_to_record):
    if not os.path.exists(path_to_students):
        logging.info("File for students doesn't exist")
        return False

    student_df = pandas.read_csv(path_to_students)
    amount_of_students_fp = open(path_to_record, 'r+')
    line = amount_of_students_fp.readline()
    old_amount_of_students = int(line)

    amount_of_students = student_df.shape[0]
    if amount_of_students > old_amount_of_students:
        logging.info(f"There are {amount_of_students - old_amount_of_students} new students.")
        amount_of_students_fp.seek(0)
        amount_of_students_fp.write(str(amount_of_students))
        amount_of_students_fp.truncate()
        return True

    return False


def transform_data(path_to_students, path_to_transformed_data, target_table, **context):
    student_df = pandas.read_csv(path_to_students)
    student_df = student_df[student_df["aprovado"].isnull()]
    student_df["aprovado"] = (student_df["nota_matematica"] > 7) & (
            student_df["nota_portugues"] > 7)

    student_df.to_csv(path_to_transformed_data)

    sql_texts = []
    for index, row in student_df.iterrows():
        sql_texts.append(
            'INSERT INTO ' + target_table + ' (' + str(
                ', '.join(student_df.columns)) + ') VALUES ' + str(tuple(row.values)) + ";")

    task_instance = context['task_instance']
    insert_query = '\n\n'.join(sql_texts)
    task_instance.xcom_push(key="the_message", value=insert_query)


dag = DAG("students_pipeline_v2.0", schedule_interval=timedelta(minutes=5),
          start_date=datetime(2020, 6, 13))

with dag:
    csv_sensor = PythonSensor(
        task_id="new_student_sensor",
        poke_interval=10,
        execution_timeout=timedelta(seconds=60),
        python_callable=detect_new_student,
        op_kwargs={"path_to_students": "data/students.csv",
                   "path_to_record": "data/student_amount.txt"},
        retries=3,
        soft_fail=True,
        retry_delay=timedelta(seconds=10))

    transformer_operator = PythonOperator(
        task_id="transform_student_data",
        python_callable=transform_data,
        op_kwargs={"path_to_students": "data/students.csv",
                   "path_to_transformed_data": "data/students_transformed.csv",
                   "target_table": "students"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(seconds=10))

    load_operator = PostgresOperator(
        task_id="load_data_into_pg",
        sql="{{ task_instance.xcom_pull(task_ids='transform_student_data', key='the_message') }}",
        postgres_conn_id='postgres_conn',
        retries=3,
        retry_delay=timedelta(seconds=10))

    csv_sensor >> transformer_operator >> load_operator
