from contextlib import contextmanager
from dataclasses import dataclass
from dbt.adapters.base import Credentials
from dbt.adapters.base import BaseConnectionManager
from dbt.clients import agate_helper
from dbt.contracts.connection import AdapterResponse, Connection
from dbt.logger import GLOBAL_LOGGER as logger

import agate
import dbt
import rockset
from rockset import Client, Q, F, sql
from typing import Any, Dict, List, Optional, Tuple, Union


@dataclass
class RocksetCredentials(Credentials):
    api_key: str
    database: Optional[str]
    api_server: Optional[str] = 'api.rs2.usw2.rockset.com'

    @property
    def type(self):
        return 'rockset'

    @property
    def unique_field(self):
        return self.api_key

    def _connection_keys(self):
        return ('api_key', 'workspace', 'schema')

    @classmethod
    def __pre_deserialize__(cls, d: Dict[Any, Any]) -> Dict[Any, Any]:
        # `database` is not a required property in Rockset. Dbt still expects to
        # see it in the credentials class in certain places, so put an arbitrary value
        if 'database' not in d:
            d['database'] = 'doesnt-matter'
        return d

    _ALIASES = {
        'workspace': 'schema'
    }


class RocksetConnectionManager(BaseConnectionManager):
    TYPE = 'rockset'

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        logger.info(f'Opening connection to Rockset')
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
            return connection
        except Exception as e:
            connection.state = 'fail'
            connection.handle = None
            raise dbt.exceptions.FailedToConnectException(e)

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
        logger.info(f'Clearing transaction')

    # auto_begin is ignored in Rockset, and only included for consistency
    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[Union[AdapterResponse, str], agate.Table]:
        sql = self._add_query_comment(sql)
        cursor = self.get_thread_connection().handle.cursor()

        if fetch:
            rows, field_names = self._sql_to_results(cursor, sql)
            table = agate_helper.table_from_data_flat(rows, field_names)
        else:
            cursor.execute(sql)
            table = agate_helper.empty_table()

        return AdapterResponse(_message='OK'), table

    def _sql_to_results(self, cursor, sql):
        cursor.execute(sql)
        field_names = self._description_to_field_names(cursor.description)
        json_results = []
        rows = cursor.fetchall()
        for row in rows:
            json_results.append(self._row_to_json(row, field_names))
        return json_results, field_names

    def _row_to_json(self, row, field_names):
        json_res = {}
        for i in range(len(row)):
            json_res[field_names[i]] = row[i]
        return json_res

    def _description_to_field_names(self, description):
        return [desc[0] for desc in description]

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except Exception as e:
            raise e

    @classmethod
    def get_response(cls, cursor):
        return 'OK'
