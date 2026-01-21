import mysql.connector
import os


def get_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host=os.environ["MYSQL_HOST"],
            port=int(os.environ["MYSQL_PORT"]),
            database=os.environ["MYSQL_DATABASE"],
            user=os.environ["MYSQL_USER"],
            password=os.environ["MYSQL_PASSWORD"],
            autocommit=True,
        )
    except Exception as e:
        raise e

    return connection
