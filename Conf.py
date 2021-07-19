import configparser


class Conf:
    def __init__(self, logger, local=False):
        self.logger = logger
        self.config = configparser.ConfigParser()
        if not local:
            self.config.read('db_config.ini')
        else:
            self.config.read('local_db_config.ini')

    def get_db(self):
        try:
            driver = self.config.get('db', 'driver')
            host = self.config.get('db', 'host')
            db = self.config.get('db', 'db')
            user = self.config.get('db', 'user')
            password = self.config.get('db', 'password')
            connection = self.config.get('db', 'connection')

            return {
                    'driver': driver,
                    'host': host,
                    'db': db,
                    'user': user,
                    'password': password,
                    'connection': connection,
            }

        except Exception as e:
            self.logger.error('Error in Configuration ... ' + repr(e))
