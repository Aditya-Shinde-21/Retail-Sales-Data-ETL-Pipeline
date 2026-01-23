import os
from scripts.main.utility.logging_config import *

class DatabaseWriter:
    def __init__(self):
        self.url = (
            f"jdbc:mysql://{os.environ['MYSQL_HOST']}:{os.environ['MYSQL_PORT']}/"
            f"{os.environ['MYSQL_DATABASE']}?useSSL=false&allowPublicKeyRetrieval=true"
        )

        self.properties = {
            "user": os.environ["MYSQL_USER"],
            "password": os.environ["MYSQL_PASSWORD"],
            "driver": "com.mysql.cj.jdbc.Driver"
        }

    def write_dataframe(self,df,table_name):
        try:
            df.write.jdbc(url = self.url,
                          table = table_name,
                          mode = "append",
                          properties = self.properties)
            logger.info(f"Data successfully written into {table_name} table ")


        except Exception:
            logger.exception("Failed to write DataFrame to MySQL table: %s", table_name)
            raise