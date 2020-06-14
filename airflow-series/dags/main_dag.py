import logging
import os
from datetime import timedelta, datetime

import pandas
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


def detect_new_student(path_to_students, path_to_record):
    if not os.path.exists(path_to_students):
        logging.info("File for students doesn't exist")
        return False

    student_df = pandas.read_csv(path_to_students)
    amount_of_students_fp = open(path_to_record, 'r')
    line = amount_of_students_fp.readline()
    amount_of_students = int(line)

    if student_df.shape[0] > amount_of_students:
        logging.info(f"There are {student_df.shape[0] - amount_of_students} new students.")
        return True

    return False


def insert_rows():
    pass


def transform_data(path_to_students, path_to_transformed_data):
    student_df = pandas.read_csv(path_to_students)
    student_df["passed"] = (student_df["nota_matematica"] > 7) & (student_df["nota_portugues"] > 7)

    student_df.to_csv(path_to_transformed_data)


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
        retries=3)

    transformation_operator = PythonOperator(
        task_id="transform_student_data",
        python_callable=transform_data,
        op_kwargs={"path_to_students": "data/students.csv",
                   "path_to_transformed_data": "data/students_transformed.csv"},
        retries=3)

    load_operator = PythonOperator(
        task_id="load_csv_into_pg",
        python_callable=insert_rows,
        retries=3)

    csv_sensor >> transformation_operator >> load_operator
