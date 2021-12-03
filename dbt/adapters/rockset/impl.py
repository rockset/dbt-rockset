from dbt.adapters.base import (
    BaseAdapter, available, RelationType
)
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.rockset.connections import RocksetConnectionManager
from dbt.adapters.rockset.relation import RocksetQuotePolicy, RocksetRelation
from dbt.contracts.graph.manifest import Manifest
from dbt.adapters.rockset.column import RocksetColumn
from dbt.logger import GLOBAL_LOGGER as logger

import agate
import datetime
import dbt
import json
import collections
import os
import requests
import rockset
from decimal import Decimal
from time import sleep, time
from typing import List, Optional, Set


OK = 200
NOT_FOUND = 404


class RocksetAdapter(BaseAdapter):
    RELATION_TYPES = {
        'TABLE': RelationType.Table,
    }

    Relation = RocksetRelation
    Column = RocksetColumn
    ConnectionManager = RocksetConnectionManager

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(
        cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float" if decimals else "int"

    @classmethod
    def convert_boolean_type(
            cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        return "bool"

    @classmethod
    def convert_datetime_type(
            cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        return "datetime"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "time"

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    @classmethod
    def date_function(cls):
        return "CURRENT_TIMESTAMP()"

    def create_schema(self, relation: RocksetRelation) -> None:
        rs = self._rs_client()
        logger.debug('Creating workspace "{}"', relation.schema)
        rs.Workspace.create(relation.schema)

    def drop_schema(self, relation: RocksetRelation) -> None:
        rs = self._rs_client()
        ws = relation.schema
        logger.info('Dropping workspace "{}"', ws)

        # Drop all views in the ws
        for view in self._list_views(ws):
            self._delete_view_recursively(ws, view)

        # Drop all aliases in the ws
        for alias in rs.Alias.list(workspace=ws):
            self._delete_alias(ws, alias.name)

        # Drop all collections in the ws
        for collection in rs.Collection.list(workspace=ws):
            self._delete_collection(
                ws, collection.name, wait_until_deleted=False)

        try:
            # Wait until the ws has 0 collections. We do this so deletion of multiple collections
            # can happen in parallel
            while True:
                workspace = rs.Workspace.retrieve(ws)
                if workspace.collection_count == 0:
                    break
                logger.debug(
                    f'Waiting for ws {ws} to have 0 collections, has {workspace.collection_count}')
                sleep(3)

            rs.Workspace.delete(ws)
        except Exception as e:
            logger.debug(f'Caught exception of type {e.__class__}')
            # Workspace does not exist
            if isinstance(e, rockset.exception.Error) and e.code == NOT_FOUND:
                pass
            else:  # Unexpected error
                raise e

    # Required by BaseAdapter
    @available
    def list_schemas(self, database: str) -> List[str]:
        rs = self._rs_client()
        return [ws.name for ws in rs.Workspace.list()]

    # Relation/Collection related methods
    def truncate_relation(self, relation: RocksetRelation) -> None:
        raise dbt.exceptions.NotImplementedException(
            '`truncate` is not implemented for this adapter!'
        )

    @available.parse(lambda *a, **k: '')
    def get_dummy_sql(self):
        return f'''
            /* Rockset does not support `CREATE TABLE AS` statements, so all SQL is executed
               dynamically in the adapter, instead of as traditionally compiled dbt models. A
               dummy query is executed instead */
            SELECT 1;
        '''

    @available.parse_list
    def drop_relation(self, relation: RocksetRelation) -> None:
        ws = relation.schema
        identifier = relation.identifier

        if self._does_view_exist(ws, identifier):
            self._delete_view_recursively(ws, identifier)
        elif self._does_collection_exist(ws, identifier):
            self._delete_collection(ws, identifier)
        else:
            raise dbt.exceptions.Exception(
                f'Tried to drop relation {ws}.{identifier} that does not exist!'
            )

    def rename_relation(
        self, from_relation: RocksetRelation, to_relation: RocksetRelation
    ) -> None:
        raise dbt.exceptions.NotImplementedException(
            '`rename` is not implemented for this adapter!'
        )

    @available.parse(lambda *a, **k: '')
    def get_collection(self, relation) -> RocksetRelation:
        ws = relation.schema
        cname = relation.identifier

        try:
            rs = self._rs_client()
            existing_collection = rs.Collection.retrieve(cname, workspace=ws)
            return self._rs_collection_to_relation(existing_collection)
        except Exception as e:
            if e.code == NOT_FOUND:  # Collection does not exist
                return None
            else:  # Unexpected error
                raise e

    # Required by BaseAdapter
    def list_relations_without_caching(
        self, schema_relation: RocksetRelation
    ) -> List[RocksetRelation]:
        # Due to the database matching issue, we can not implement relation caching, so
        # this is a simple pass-through to list_relations
        return self.list_relations(None, schema_relation.schema)

    # We override `list_relations` bc the base implementation uses a caching mechanism that does
    # not work for our purposes bc it relies on comparing relation.database, and database is not
    # a concept in Rockset
    def list_relations(
        self, database: Optional[str], schema: str
    ) -> List[RocksetRelation]:

        rs = self._rs_client()
        relations = []

        collections = rs.Collection.list(workspace=schema)
        for collection in collections:
            relations.append(self._rs_collection_to_relation(collection))
        return relations

    # Required by BaseAdapter
    def get_columns_in_relation(
        self, relation: RocksetRelation
    ) -> List[RocksetColumn]:
        logger.debug(f'Getting columns in relation {relation.identifier}')
        sql = 'DESCRIBE "{}"."{}"'.format(relation.schema, relation.identifier)
        status, table = self.connections.execute(sql, fetch=True)

        columns = []
        for row in table.rows:
            top_lvl_field = json.loads(row['field'])
            if len(top_lvl_field) == 1:
                col = self.Column.create(top_lvl_field[0], row['type'])
                columns.append(col)
        return columns

    def _get_types_in_relation(
        self, relation: RocksetRelation
    ) -> List[RocksetColumn]:
        logger.debug(f'Getting columns in relation {relation.identifier}')
        if not (relation.schema and relation.identifier):
            return []
        sql = 'DESCRIBE "{}"."{}"'.format(relation.schema, relation.identifier)
        status, table = self.connections.execute(sql, fetch=True)

        field_types = collections.defaultdict(list)
        for row in table.rows:
            r = json.loads(row['field'])
            field_path = '.'.join(r)
            field_types[field_path].append((row['occurrences'], row['type']))

        # Sort types by their relative frequency
        return [{"column_name": k, "column_type": '/'.join([t[1] for t in sorted(v, reverse=True)])} for k, v in field_types.items()]

    # Rockset doesn't support DESCRIBE on views, so those are not included in the catalog information
    def get_catalog(self, manifest: Manifest) -> agate.Table:
        schemas = super()._get_cache_schemas(manifest)
        rs = self._rs_client()

        columns = ['table_database', 'table_name', 'table_schema', 'table_type', 'stats:row_count:label', 'stats:row_count:value', 'stats:row_count:description',
                   'stats:row_count:include', 'stats:bytes:label', 'stats:bytes:value', 'stats:bytes:description', 'stats:bytes:include', 'column_type', 'column_name', 'column_index']
        catalog_rows = []
        for relation in schemas:
            for collection in rs.Collection.list(workspace=relation.schema):
                rel = self.Relation.create(
                    schema=collection.workspace, identifier=collection.name)
                col_types = self._get_types_in_relation(rel)
                for i, c in enumerate(col_types):
                    catalog_rows.append(['', collection.name, collection.workspace, 'NoSQL', 'Row Count', collection.stats['doc_count'], 'Rows in table',
                                        True, 'Bytes', collection.stats['bytes_inserted'], 'Inserted bytes', True, c['column_type'], c['column_name'], i])
        catalog_table = agate.Table(rows=catalog_rows, column_names=columns, column_types=agate.TypeTester(
            force={'table_database': agate.Text(cast_nulls=False, null_values=[])}))

        return catalog_table, []

    def expand_column_types(
        self, goal: RocksetRelation, current: RocksetRelation
    ) -> None:
        raise dbt.exceptions.NotImplementedException(
            '`expand_column_types` is not implemented for this adapter!'
        )

    def expand_target_column_types(
        self, from_relation: RocksetRelation, to_relation: RocksetRelation
    ) -> None:
        raise dbt.exceptions.NotImplementedException(
            '`expand_target_column_types` is not implemented for this adapter!'
        )

    @classmethod
    def quote(cls, identifier: str) -> str:
        return '`{}`'.format(identifier)

    ###
    # Special Rockset implementations
    ###

    # Table materialization
    @available.parse(lambda *a, **k: '')
    def create_table(self, relation, sql):
        ws = relation.schema
        cname = relation.identifier
        rs = self._rs_client()

        if self._does_collection_exist(ws, cname):
            self._delete_collection(ws, cname)

        if self._does_alias_exist(ws, cname):
            self._delete_alias(ws, cname)

        if self._does_view_exist(ws, cname):
            self._delete_view_recursively(ws, cname)

        logger.debug(f'Creating collection {ws}.{cname}')

        c = rs.Collection.create(
            cname,
            workspace=ws
        )
        self._wait_until_collection_ready(ws, cname)

        # Run an INSERT INTO statement and wait for it to be fully ingested
        relation = self._rs_collection_to_relation(c)
        iis_query_id = self._execute_iis_query_and_wait_for_docs(relation, sql)

    # Used by dbt seed
    @available.parse_none
    def load_dataframe(self, database, schema, table_name, agate_table, column_override):
        ws = schema
        cname = table_name

        # Translate the agate table in json docs
        json_docs = []
        for row in agate_table.rows:
            d = dict(row.dict())
            for k, v in d.items():
                d[k] = self._convert_agate_data_type(v)
            json_docs.append(d)

        # The check for a view should happen before this point
        if self._does_view_exist(ws, cname):
            raise dbt.exceptions.Exception(
                f'InternalError : View {ws}.{cname} exists')

        # Create the Rockset collection
        if not self._does_collection_exist(ws, cname):
            rs = self._rs_client()
            rs.Collection.create(
                cname,
                workspace=ws
            )
        self._wait_until_collection_ready(ws, cname)

        # Write the results to the collection and wait until the docs are ingested
        body = {'data': json_docs}
        write_api_endpoint = f'/v1/orgs/self/ws/{ws}/collections/{cname}/docs'
        resp = json.loads(self._send_rs_request(
            'POST', write_api_endpoint, body).text)
        self._wait_until_past_commit_fence(ws, cname, resp['last_offset'])

    def _convert_agate_data_type(self, v):
        if isinstance(v, str):
            return v
        elif isinstance(v, Decimal):
            return float(v)
        elif isinstance(v, datetime.datetime):
            return str(v)
        elif isinstance(v, datetime.date):
            return str(v)
        else:
            raise dbt.exceptions.Exception(
                f'Unknown data type {v.__class__} in seeded table')

    # View materialization
    # As of this comment, the rockset python sdk does not support views, so this is implemented
    # with the python requests library
    @available.parse(lambda *a, **k: '')
    def create_view(self, relation, sql):
        ws = relation.schema
        view = relation.identifier

        if not self._does_view_exist(ws, view):
            self._create_view(ws, view, sql)
        else:
            self._update_view(ws, view, sql)

        # If we wait until the view is synced, then we can be sure that any subsequent queries
        # of the view will use the new sql text
        self._wait_until_view_fully_synced(ws, view)

        # Sleep a few seconds to be extra sure that all caches are updated with the new view
        sleep(3)

    @available.parse(lambda *a, **k: '')
    def add_incremental_docs(self, relation, sql, unique_key):
        if unique_key and unique_key != '_id':
            raise dbt.exceptions.NotImplementedException(
                '`unique_key` can only be set to `_id` with the Rockset adapter!'
            )

        ws = relation.schema
        cname = relation.identifier
        self._wait_until_collection_ready(ws, cname)

        # Run an INSERT INTO statement and wait for it to be fully ingested
        iis_query_id = self._execute_iis_query_and_wait_for_docs(relation, sql)

    ###
    # Internal Rockset helper methods
    ###

    def _rs_client(self):
        return self.connections.get_thread_connection().handle._client()

    def _rs_api_key(self):
        return self.connections.get_thread_connection().credentials.api_key

    def _rs_api_server(self):
        return f'https://{self.connections.get_thread_connection().credentials.api_server}'

    def _rs_cursor(self):
        return self.connections.get_thread_connection().handle.cursor()

    def _execute_iis_query_and_wait_for_docs(self, relation, sql):
        query_id, num_docs_inserted = self._execute_iis_query(relation, sql)
        if num_docs_inserted > 0:
            self._wait_until_iis_query_processed(
                relation.schema, relation.identifier, query_id)
        else:
            logger.info(
                f'Query {query_id} inserted 0 docs; no ingest to wait for.')

    # Execute a query not using the SQL cursor, but by hitting the REST api. This should be done
    # if you need the QueryResponse object returned
    # Returns: query_id (str), num_docs_inserted (int)
    def _execute_iis_query(self, relation, sql):
        iis_sql = f'INSERT INTO {relation} {sql}'
        logger.debug(f'Executing sql: {iis_sql}')
        endpoint = '/v1/orgs/self/queries'
        body = {'sql': {'query': iis_sql}}
        resp = self._send_rs_request('POST', endpoint, body=body)
        if resp.status_code != OK:
            raise dbt.exceptions.Exception(resp.text)

        json_resp = json.loads(resp.text)
        assert len(json_resp['results']) == 1

        return json_resp['query_id'], json_resp['results'][0]['num_docs_inserted']

    def _wait_until_collection_does_not_exist(self, cname, ws):
        while True:
            try:
                c = self._rs_client().Collection.retrieve(cname, workspace=ws)
                logger.debug(
                    f'Waiting for collection {ws}.{cname} to be deleted...')
                sleep(3)
            except Exception as e:
                if e.code == NOT_FOUND:  # Collection does not exist
                    return
                raise e

    def _wait_until_view_does_not_exist(self, ws, view):
        while True:
            if self._does_view_exist(ws, view):
                logger.debug(f'Waiting for view {ws}.{view} to be deleted')
                sleep(3)
            else:
                break

    def _wait_until_collection_ready(self, ws, cname):
        max_wait_time_secs = 600
        sleep_secs = 3
        total_sleep_time = 0

        while total_sleep_time < max_wait_time_secs:
            if not self._does_collection_exist(ws, cname):
                logger.debug(
                    f'Collection {ws}.{cname} does not exist. This is likely a transient consistency error.')
                sleep(sleep_secs)
                total_sleep_time += sleep_secs
                continue

            c = self._rs_client().Collection.retrieve(
                cname,
                workspace=ws
            )
            if c.describe().data['status'] == 'READY':
                logger.debug(f'{ws}.{cname} is ready!')
                return
            else:
                logger.debug(
                    f'Waiting for collection {ws}.{cname} to become ready...')
                sleep(sleep_secs)
                total_sleep_time += sleep_secs

        raise dbt.exceptions.Exception(
            f'Waited more than {max_wait_time_secs} secs for {ws}.{cname} to become ready. Something is wrong.'
        )

    def _rs_collection_to_relation(self, collection):
        if collection is None:
            return None

        return self.Relation.create(
            database=None,
            schema=collection.workspace,
            identifier=collection.name,
            type='table',
            quote_policy=RocksetQuotePolicy()
        )

    def _wait_until_alias_deleted(self, ws, alias):
        while True:
            if self._does_alias_exist(ws, alias):
                logger.debug(f'Waiting for alias {ws}.{alias} to be deleted')
                sleep(3)
            else:
                break

    def _wait_until_collection_deleted(self, ws, cname):
        while True:
            if self._does_collection_exist(ws, cname):
                logger.debug(
                    f'Waiting for collection {ws}.{cname} to be deleted')
                sleep(3)
            else:
                break

    def _delete_collection(self, ws, cname, wait_until_deleted=True):
        rs = self._rs_client()

        for ref_view in self._get_referencing_views(ws, cname):
            self._delete_view_recursively(ref_view[0], ref_view[1])

        try:
            c = rs.Collection.retrieve(cname, workspace=ws)
            c.drop()

            if wait_until_deleted:
                self._wait_until_collection_deleted(ws, cname)
        except Exception as e:
            if e.code != NOT_FOUND:
                raise e  # Unexpected error

    def _delete_alias(self, ws, alias):
        rs = self._rs_client()

        for ref_view in self._get_referencing_views(ws, alias):
            self._delete_view_recursively(ref_view[0], ref_view[1])

        try:
            a = rs.Alias.retrieve(alias, workspace=ws)
            a.drop()
            self._wait_until_alias_deleted(ws, alias)
        except Exception as e:
            if e.code != NOT_FOUND:
                raise e  # Unexpected error

    def _wait_until_past_commit_fence(self, ws, cname, fence):
        endpoint = f'/v1/orgs/self/ws/{ws}/collections/{cname}/offsets/commit?fence={fence}'
        while True:
            resp = self._send_rs_request('GET', endpoint)
            resp_json = json.loads(resp.text)
            passed = resp_json['data']['passed']
            commit_offset = resp_json['offsets']['commit']
            if passed:
                logger.debug(
                    f'Commit offset {commit_offset} is past given fence {fence}')
                break
            else:
                logger.debug(
                    f'Waiting for commit offset to pass fence {fence}; it is currently {commit_offset}')
                sleep(3)

    def _wait_until_iis_fully_ingested(self, ws, cname, query_id):
        endpoint = f'/v1/orgs/self/queries/{query_id}'
        while True:
            query_resp = self._send_rs_request('GET', endpoint)
            last_offset = json.loads(query_resp.text)['last_offset']
            if last_offset is not None:
                return last_offset
            else:
                logger.debug(
                    f'Insert Into Query not yet finished processing; last offset not present')
                sleep(3)

    def _wait_until_iis_query_processed(self, ws, cname, query_id):
        last_offset = self._wait_until_iis_fully_ingested(ws, cname, query_id)
        self._wait_until_past_commit_fence(ws, cname, last_offset)

    def _send_rs_request(self, type, endpoint, body=None, check_success=True):
        url = self._rs_api_server() + endpoint
        headers = {"authorization": f'apikey {self._rs_api_key()}'}

        if type == 'GET':
            resp = requests.get(url, headers=headers)
        elif type == 'POST':
            resp = requests.post(url, headers=headers, json=body)
        elif type == 'DELETE':
            resp = requests.delete(url, headers=headers)
        else:
            raise Exception(f'Unimplemented request type {type}')

        code = resp.status_code
        if check_success and (code < 200 or code > 299):
            raise Exception(resp.text)
        return resp

    def _views_endpoint(self, ws):
        return f'/v1/orgs/self/ws/{ws}/views'

    def _list_views(self, ws):
        endpoint = self._views_endpoint(ws)
        resp_json = json.loads(self._send_rs_request('GET', endpoint).text)
        return [v['name'] for v in resp_json['data']]

    def _does_view_exist(self, ws, view):
        endpoint = self._views_endpoint(ws) + f'/{view}'
        response = self._send_rs_request('GET', endpoint, check_success=False)
        if response.status_code == NOT_FOUND:
            return False
        elif response.status_code == OK:
            return True
        else:
            raise Exception(
                f'throwing from 332 with status_code {response.status_code} and text {response.text}')

    def _does_alias_exist(self, ws, alias):
        rs = self._rs_client()
        try:
            rs.Alias.retrieve(
                workspace=ws,
                name=alias
            )
            return True
        except Exception as e:
            if isinstance(e, rockset.exception.InputError) and e.code == NOT_FOUND:
                return False
            else:
                raise e

    def _does_collection_exist(self, ws, cname):
        rs = self._rs_client()
        try:
            rs.Collection.retrieve(
                workspace=ws,
                name=cname
            )
            return True
        except Exception as e:
            if isinstance(e, rockset.exception.InputError) and e.code == NOT_FOUND:
                return False
            else:
                raise e

    def _create_view(self, ws, view, sql):
        # Check if alias or collection exist with same name
        rs = self._rs_client()
        if self._does_alias_exist(ws, view):
            self._delete_alias(ws, view)

        if self._does_collection_exist(ws, view):
            self._delete_collection(ws, view)

        endpoint = self._views_endpoint(ws)
        body = {
            'name': view,
            'query': sql,
            'description': 'Created via dbt'
        }
        self._send_rs_request('POST', endpoint, body=body)

    # Delete the view and any views that depend on it (recursively)
    def _delete_view_recursively(self, ws, view):
        for ref_view in self._get_referencing_views(ws, view):
            self._delete_view_recursively(ref_view[0], ref_view[1])

        endpoint = f'{self._views_endpoint(ws)}/{view}'
        del_resp = self._send_rs_request('DELETE', endpoint)
        if del_resp.status_code == NOT_FOUND:
            return
        elif del_resp.status_code != OK:
            raise Exception(
                f'throwing from 395 with code {del_resp.status_code} and text {del_resp.text}')

        self._wait_until_view_does_not_exist(ws, view)

    def _get_referencing_views(self, ws, view):
        view_path = f'{ws}.{view}'

        list_endpoint = f'{self._views_endpoint(ws)}'
        list_resp = self._send_rs_request('GET', list_endpoint)
        list_json = json.loads(list_resp.text)

        results = []
        for view in list_json['data']:
            for referenced_entity in view['entities']:
                if referenced_entity == view_path:
                    results.append((view['workspace'], view['name']))
        return results

    def _update_view(self, ws, view, sql):
        endpoint = self._views_endpoint(ws) + f'/{view}'
        body = {'query': sql}
        self._send_rs_request('POST', endpoint, body=body)

    def _wait_until_view_fully_synced(self, ws, view):
        max_wait_time_secs = 600
        sleep_secs = 3
        total_sleep_time = 0

        endpoint = f'{self._views_endpoint(ws)}/{view}'
        while total_sleep_time < max_wait_time_secs:
            if not self._does_view_exist(ws, view):
                logger.debug(
                    f'View {ws}.{cname} does not exist. This is likely a transient consistency error.')
                sleep(sleep_secs)
                total_sleep_time += sleep_secs
                continue

            resp = self._send_rs_request('GET', endpoint)
            view_json = json.loads(resp.text)['data']
            state = view_json['state']

            if state == 'SYNCING':
                logger.debug(
                    f'Waiting for view {ws}.{view} to be fully synced')
                sleep(sleep_secs)
                total_sleep_time += sleep_secs
            else:
                logger.debug(
                    f'View {ws}.{view} is synced and ready to be queried')
                return

        raise dbt.exceptions.Exception(
            f'Waited more than {max_wait_time_secs} secs for view {ws}.{view} to become synced. Something is wrong.'
        )

    # Overridden because Rockset generates columns not added during testing.
    def get_rows_different_sql(
        self,
        relation_a: RocksetRelation,
        relation_b: RocksetRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = 'EXCEPT',
    ) -> str:

        names: List[str]
        # columns generated by Rockset
        skip_cmp_columns: Set[str] = {'_event_time', '_id', '_meta'}
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name)
                           for c in columns if c.name not in skip_cmp_columns))
        else:
            names = sorted((self.quote(n)
                           for n in column_names if n not in skip_cmp_columns))
        columns_csv = ', '.join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
            except_op=except_operator,
        )

        return sql


COLUMNS_EQUAL_SQL = '''
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} {except_op}
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} {except_op}
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a, table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
join diff_count using (id)
'''.strip()
