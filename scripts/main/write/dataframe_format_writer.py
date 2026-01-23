import traceback
from scripts.main.utility.logging_config import *
class FormatWriter:
    def __init__(self,mode,data_format):
        self.mode = mode
        self.data_format = data_format

    def write_to_format(self,df, file_path):
        try:
            df.write.format(self.data_format) \
                .option("header", "true") \
                .mode(self.mode) \
                .option("path", file_path) \
                .save()

        except Exception:
            logger.exception("Failed to write DataFrame to %s format at path %s", self.data_format, file_path)
            raise