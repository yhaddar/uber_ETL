import logging
import os

class Logs:
    def __init__(self):
        os.makedirs("logs", exist_ok=True)

    @staticmethod
    def get_logger(name):

        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            file_handler = logging.FileHandler(f"logs/uber.log")
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        return logger

