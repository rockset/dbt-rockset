from contextlib import contextmanager
from dataclasses import dataclass
from dbt.adapters.base import Credentials
from dbt.adapters.base import BaseConnectionManager
from dbt.clients import agate_helper
from dbt.contracts.connection import AdapterResponse, Connection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.exceptions import NotImplementedError, DbtValidationError

import agate
import dbt
import rockset_sqlalchemy as sql
from .__version__ import version as rs_version
from typing import Any, Dict, List, Optional, Tuple, Union


@dataclass
class RocksetCredentials(Credentials):
    database: str = "db"
    vi_rrn: Optional[str] = None
    run_async_iis: Optional[bool] = False
    api_server: Optional[str] = "api.usw2a1.rockset.com"
    api_key: Optional[str] = None
    schema: Optional[str] = None

    @property
    def type(self):
        return "rockset"

    @property
    def unique_field(self):
        return self.api_key

    def _connection_keys(self):
        return ("api_key", "apiserver", "schema")

    _ALIASES = {"workspace": "schema"}


class RocksetConnectionManager(BaseConnectionManager):
    TYPE = "rockset"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        # Ensure the credentials have a valid apiserver before connecting to rockset
        if not (
            credentials.api_server is not None
            and "api" in credentials.api_server
            and credentials.api_server.endswith("rockset.com")
        ):
            raise DbtValidationError(
                f"Invalid apiserver `{credentials.api_server}` specified in profile. Expecting a server of the form api.<region>.rockset.com"
            )

        try:
            handle = sql.connect(
                api_server=credentials.api_server, api_key=credentials.api_key
            )
            handle._client.api_client.user_agent = "dbt/" + rs_version

            connection.state = "open"
            connection.handle = handle
            return connection
        except Exception as e:
            connection.state = "fail"
            connection.handle = None
            raise dbt.exceptions.FailedToConnectException(e)

    @classmethod
    def get_status(cls, cursor) -> str:
        # Rockset cursors don't have a status_message
        return "OK"

    def cancel_open(self) -> Optional[List[str]]:
        raise NotImplementedError("`cancel_open` is not implemented for this adapter!")

    def begin(self) -> None:
        """Begin a transaction. (passable)"""
        raise NotImplementedError("`begin` is not implemented for this adapter!")

    def commit(self) -> None:
        """Commit a transaction. (passable)"""
        raise NotImplementedError("`commit` is not implemented for this adapter!")

    def clear_transaction(self) -> None:
        pass

    # auto_begin is ignored in Rockset, and only included for consistency
    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[Union[AdapterResponse, str], agate.Table]:
        sql = self._add_query_comment(sql)
        cursor = self.get_thread_connection().handle.cursor()

        if fetch:
            rows, field_names = self._sql_to_results(cursor, sql, limit)
            table = agate_helper.table_from_data_flat(rows, field_names)
        else:
            cursor.execute(sql)
            table = agate_helper.empty_table()

        return AdapterResponse(_message="OK"), table

    def _sql_to_results(self, cursor, sql, limit):
        cursor.execute(sql)
        field_names = self._description_to_field_names(cursor.description)
        json_results = []
        if limit is None:
            rows = cursor.fetchall()
        else:
            rows = cursor.fetchmany(limit)
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
    def get_response(cls, cursor) -> str:
        return "OK"
