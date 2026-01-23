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
            autocommit=False,
            raise_on_warnings=True,
            connection_timeout=30,
            charset="utf8mb4",
            collation="utf8mb4_unicode_ci"
        )

    except mysql.connector.Error as err:
        raise RuntimeError(f"MySQL connection failed: {err}") from err

    return connection