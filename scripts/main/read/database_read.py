import os
from scripts.main.utility.logging_config import logger


class DatabaseReader:
    def __init__(self):
        self.url = (f"jdbc:mysql://{os.environ['MYSQL_HOST']}:{os.environ['MYSQL_PORT']}/"
                    f"{os.environ['MYSQL_DATABASE']}?useSSL=false&allowPublicKeyRetrieval=true")

        self.properties = {"user": os.environ["MYSQL_USER"],
                           "password": os.environ["MYSQL_PASSWORD"],
                           "driver": "com.mysql.cj.jdbc.Driver"}

    def create_dataframe(self,spark,table_name):
        try:
            df = spark.read.jdbc(url=self.url,
                                 table=table_name,
                                 properties=self.properties)
            return df

        except Exception:
            logger.exception("Failed to read MySQL table '%s' using Spark JDBC", table_name)
            raise