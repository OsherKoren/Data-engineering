import logging


# Logger configuration:
class Mylogger:
    def __init__(self, logger_name):
        self.loggerName = logger_name

    def log(self):
        logger = logging.getLogger(self.loggerName)
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s")

        # Create handlers and set levels
        if not logger.handlers:
            file_handler = logging.FileHandler(self.loggerName + '.log')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)

            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.CRITICAL)
            console_handler.setFormatter(formatter)

            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        return logger
