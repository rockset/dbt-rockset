from dataclasses import dataclass

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager

import rockset

@dataclass
class RocksetCredentials(Credentials):
    api_server: str
    api_key: str

    @property
    def type(self):
        return 'rockset'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'.
        return ('api_server')


class RocksetConnectionManager(SQLConnectionManager):
    TYPE = 'rockset'

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials

        try:
            handle = rockset.sql.connect(
                api_server=credentials.api_server,
                api_key=credentials.api_key,
            )
            connection.state = 'open'
            connection.handle = handle
        return connection

    @classmethod
    def get_status(cls, cursor):
        # Rockset cursor's don't have a status_message
        return 'OK'

    def cancel(self, connection):
        pass

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except rockset.sql.ProgrammingError as e:
            logger.debug('Invalid user request: {}'.format(str(e)))
            raise dbt.exceptions.CompilationException(str(e))
        except rockset.sql.NotSupportedError as exc:
            logger.debug('Feature not yet implemented: {}'.format(str(e)))
            raise dbt.exceptions.CompilationException(str(e))
        except (rockset.sql.OperationalError,
            rockset.sql.InternalError) as e:
            logger.debug("Error running SQL: {}".format(sql))
            raise dbt.exceptions.DatabaseException(str(e))
