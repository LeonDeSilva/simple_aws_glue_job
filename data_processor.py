import json
import sys

import boto3 as boto3
import pg8000
from modules.logger_util import Logger
from awsglue.utils import getResolvedOptions

parameters = getResolvedOptions(sys.argv, ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'BUCKET_NAME', 'DATA_FILE'])
logger = Logger('data_processor')

conn = pg8000.connect(
    host=parameters["DB_HOST"],
    database=parameters["DB_NAME"],
    user=parameters["DB_USER"],
    password=parameters["DB_PASSWORD"])

bucket_name = parameters["BUCKET_NAME"]
data_file_name = parameters["DATA_FILE"]

s3_client = boto3.client('s3')


def insert_data_to_db(statement, *args):
    """
    Function to insert record to db.

    :param statement: the statement to execute
    :return: the inserted record
    """
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(statement, args)
        conn.commit()
        return cursor.fetchone()
    finally:
        if cursor is not None:
            cursor.close()


def read_data_from_s3():
    """
    Function to read data from s3 file
    """
    response = s3_client.get_object(Bucket=bucket_name, Key=data_file_name)
    return response['Body']


def process():
    """
    Function to process data
    """
    data_json = json.load(read_data_from_s3())
    logger.info("Successfully read data json: " + str(data_json))

    employee_info = data_json["employee_info"]

    for info in employee_info:
        first_name = info["first_name"]
        last_name = info["last_name"]
        date_of_birth = info["date_of_birth"]
        department_id = info["department_id"]

        insert_data_to_db(
            """
            INSERT INTO EMPLOYEE_INFO(first_name, last_name, date_of_birth, department_id)
            VALUES(%s, %s, %s, %s) RETURNING id
            """, first_name, last_name, date_of_birth, department_id)

        logger.info('Data processed for ' + first_name + " " + last_name)

    logger.info("Data processing completed")


process()
