from contextlib import contextmanager
from dataclasses import dataclass
from dbt.adapters.base import Credentials
from dbt.adapters.base import BaseConnectionManager
from dbt.clients import agate_helper
from dbt.contracts.connection import AdapterResponse, Connection

import agate
import dbt
import rockset
from rockset import Client, Q, F, sql
from typing import List, Optional, Tuple, Union


@dataclass
class RocksetCredentials(Credentials):
    api_key: str
    api_server: Optional[str] = 'api.rs2.usw2.rockset.com'

    @property
    def type(self):
        return 'rockset'

    def _connection_keys(self):
        return ('api_server', 'workspace', 'schema', 'database')

    # TODO This should work, but it doesn't. dbt says field `schema` is still required
    # in profiles.yml
    _ALIASES = {
        'workspace': 'schema'
    }


class RocksetConnectionManager(BaseConnectionManager):
    TYPE = 'rockset'

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials

        try:
            handle = sql.connect(
                api_server=credentials.api_server,
                api_key=credentials.api_key
            )

            connection.state = 'open'
            connection.handle = handle
        except Exception as e:
            print("CAUGHT EXCEPTION")
            print(e)
            connection.state = 'fail'
            connection.handle = None
        return connection

    @classmethod
    def get_status(cls, cursor):
        # Rockset cursors don't have a status_message
        return 'OK'

    def cancel_open(self) -> Optional[List[str]]:
        raise dbt.exceptions.NotImplementedException(
            '`cancel_open` is not implemented for this adapter!'
        )

    def begin(self) -> None:
        """Begin a transaction. (passable)"""
        raise dbt.exceptions.NotImplementedException(
            '`begin` is not implemented for this adapter!'
        )

    def commit(self) -> None:
        """Commit a transaction. (passable)"""
        raise dbt.exceptions.NotImplementedException(
            '`commit` is not implemented for this adapter!'
        )

    # Rockset does not implement transactions
    def clear_transaction(self) -> None:
        pass

    # auto_begin is ignored in Rockset, and only included for consistency
    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[Union[AdapterResponse, str], agate.Table]:
        sql = self._add_query_comment(sql)
        cursor = self.get_thread_connection().handle.cursor()
        cursor.execute(sql)

        if fetch:
            table = self.get_table_from_response(cursor.fetchall())
        else:
            table = agate_helper.empty_table()

        return AdapterResponse(_message='OK'), table


    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except Exception as e:
            raise e
        # "module 'rockset' has no attribute 'sql'" with code below

        # except rockset.sql.ProgrammingError as e:
        #     logger.debug('Invalid user request: {}'.format(str(e)))
        #     raise dbt.exceptions.CompilationException(str(e))
        # except rockset.sql.NotSupportedError as exc:
        #     logger.debug('Feature not yet implemented: {}'.format(str(e)))
        #     raise dbt.exceptions.CompilationException(str(e))
        # except (rockset.sql.OperationalError,
        #     rockset.sql.InternalError) as e:
        #     logger.debug("Error running SQL: {}".format(sql))
        #     raise dbt.exceptions.DatabaseException(str(e))

    @classmethod
    def get_response(cls, cursor):
        return 'OK'

    @classmethod
    def get_table_from_response(cls, resp):
        raise dbt.exceptions.NotImplementedException(
            '`get_table_from_response` not yet implemented. See BigQuery for example'
        )
