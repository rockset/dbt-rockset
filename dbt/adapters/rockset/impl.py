from dbt.adapters.base import (
    BaseAdapter, available, RelationType
)
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.rockset.connections import RocksetConnectionManager
from dbt.adapters.rockset.relation import RocksetRelation
from dbt.adapters.rockset.column import RocksetColumn
from dbt.adapters.rockset.util import sql_to_json_results
from dbt.logger import GLOBAL_LOGGER as logger

import agate
import dbt
import json
import os
import requests
import rockset
from time import sleep, time
from typing import List


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

    # Schema/workspace related methods
    def create_schema(self, relation: RocksetRelation) -> None:
        rs = self._rs_client()
        logger.info('Creating workspace "{}"', relation.schema)
        rs.Workspace.create(relation.schema)

    def drop_schema(self, relation: RocksetRelation) -> None:
        rs = self._rs_client()
        logger.info('Dropping workspace "{}"', relation.schema)
        try:
            # Drop all collections in the ws
            for collection in rs.Collection.list(workspace=relation.schema):
                collection.drop()

            # Wait until the ws has 0 collections
            while True:
                workspace = rs.Workspace.retrieve(relation.schema)
                if workspace.collection_count == 0:
                    break
                logger.info(f'Waiting for ws {relation.schema} to have 0 collections, has {workspace.collection_count}')
                sleep(5)

            # Now delete the workspace
            rs.Workspace.delete(relation.schema)
        except Exception as e:
            if e.code == 404 and e.type == 'NotFound': # Workspace does not exist
                return None
            else: # Unexpected error
                raise e

    @available.parse(lambda *a, **k: False)
    def check_schema_exists(self, database: str, schema: str) -> bool:
        logger.info(f'Checking if schema {schema} exists')
        rs = self._rs_client()
        try:
            _ = rs.Workspace.retrieve(schema)
            return True
        except:
            pass

        return False

    @available
    def list_schemas(self, database: str) -> List[str]:
        logger.info(f'Listing schemas')
        rs = self._rs_client()
        return [ws.name for ws in rs.Workspace.list()]

    # Relation/Collection related methods
    def truncate_relation(self, relation: RocksetRelation) -> None:
        raise dbt.exceptions.NotImplementedException(
            '`truncate` is not implemented for this adapter!'
        )

    @available.parse_list
    def drop_relation(self, relation: RocksetRelation) -> None:
        logger.info(f'Dropping relation {relation.schema}.{relation.identifier}')
        rs = self._rs_client()
        rs.Collection.retrieve(
            relation.identifier,
            workspace=relation.schema
        ).drop()

        self._wait_until_collection_does_not_exist(relation.identifier, relation.schema)

    def rename_relation(
        self, from_relation: RocksetRelation, to_relation: RocksetRelation
    ) -> None:
        raise dbt.exceptions.NotImplementedException(
            '`rename` is not implemented for this adapter!'
        )

    @available.parse(lambda *a, **k: '')
    def get_alias(self, relation) -> RocksetRelation:
        ws = relation.schema
        alias = relation.identifier

        try:
            rs = self._rs_client()
            existing_alias = rs.Alias.retrieve(alias, workspace=ws)
            return self._rs_alias_to_relation(existing_alias)
        except Exception as e:
            if e.code == 404 and e.type == 'NotFound': # Collection does not exist
                return None
            else: # Unexpected error
                raise e

    def list_relations_without_caching(
        self, schema_relation: RocksetRelation
    ) -> List[RocksetRelation]:

        # get Rockset client to use Rockset's Python API
        rs = self._rs_client()
        if schema_relation and schema_relation.identifier and schema_relation.schema:
            aliases = [rs.Alias.retrieve(
                schema_relation.identifier,
                workspace=schema_relation.schema,
            )]
        else:
            aliases = rs.Alias.list()

        # map Rockset aliases to RocksetRelation
        relations = []
        for alias in aliases:
            relations.append(self._rs_alias_to_relation(alias))

        return relations

    # Columns/fields related methods
    def get_columns_in_relation(
        self, relation: RocksetRelation
    ) -> List[RocksetColumn]:
        logger.info(f'Getting columns in relation {relation.identifier}')
        sql = 'DESCRIBE "{}"."{}"'.format(relation.schema, relation.identifier)
        status, table = self.connections.execute(sql, fetch=True)

        columns = []
        for row in table.rows():
            if length(row['field']) == 1:
                col = self.Column.create(row['field'][0], row['type'])
                columns.append(col)

        return columns

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

    # Used to create seed tables for testing
    @available.parse_none
    def load_dataframe(self, database, schema, table_name, agate_table,
                       column_override):
        # Translate the agate table in json docs
        json_results = []
        for row in agate_table.rows:
            d = dict(row.dict())
            for k, v in d.items():
                d[k] = str(v)
            json_results.append(d)

        # Create the Rockset collection
        rs = self._rs_client()
        c = rs.Collection.create(
            table_name,
            workspace=schema
        )
        self._wait_until_collection_ready(table_name, schema)

        # Write the results to the collection and wait until the docs are ingested
        expected_doc_count = len(json_results)
        c.add_docs(json_results)
        self._wait_until_docs(table_name, schema, expected_doc_count)

    def _create_alias(self, rs, alias, cpath):
        logger.info(f'Creating alias {alias} on collection {cpath}')
        rs.Alias.create(
            name=alias,
            workspace=cpath.split('.')[0],
            collections=[cpath]
        )

    def _wait_until_past_commit_fence(self, ws, cname, fence):
        endpoint = f'/v1/orgs/self/ws/{ws}/collections/{cname}/offsets/commit?fence={fence}'
        while True:
            resp = self._send_rs_request('GET', endpoint)
            resp_json = json.loads(resp.text)
            passed = resp_json['data']['passed']
            commit_offset = resp_json['offsets']['commit']
            if passed:
                logger.info(f'Commit offset {commit_offset} is past given fence {fence}')
                break
            else:
                logger.info(f'Waiting for commit offset to pass fence {fence}; it is currently {commit_offset}')
                sleep(3)


    def _wait_until_iis_fully_ingested(self, ws, cname, query_id):
        endpoint = f'/v1/orgs/self/queries/{query_id}'
        while True:
            query_resp = self._send_rs_request('GET', endpoint)
            last_offset = json.loads(query_resp.text)['last_offset']
            if last_offset is not None:
                self._wait_until_past_commit_fence(ws, cname, last_offset)
                break
            else:
                logger.info(f'Insert Into Query not yet finished processing; last offset not present')
                sleep(3)

    # Table materialization
    @available.parse(lambda *a, **k: '')
    def create_table(self, relation, sql):
        ws = relation.schema

        if self._does_view_exist(ws, relation.identifier):
            self._delete_view(ws, relation.identifier)

        current_millis = round(time() * 1000)
        cname_new = f'{relation.identifier}_{current_millis}'
        cpath = f'{ws}.{cname_new}'

        logger.info(f'Creating collection {ws}.{cname_new}')
        rs = self._rs_client()

        c = rs.Collection.create(
            cname_new,
            workspace=ws
        )
        self._wait_until_collection_ready(cname_new, ws)

        # Run an INSERT INTO statement and wait for it to be fully ingested
        insert_into_sql = f'''
            INSERT INTO {ws}.{cname_new}
            {sql}
        '''
        iis_query_id = self._execute_query(insert_into_sql)
        self._wait_until_iis_fully_ingested(ws, cname_new, iis_query_id)

        # Now we have a timestamp tagged table. Make sure that an alias named
        # relation.identifier is pointing to it
        alias_name = relation.identifier

        try:
            alias = rs.Alias.retrieve(alias_name, workspace=ws)

            old_collections = alias.collections
            alias.update(collections=[cpath])

            # Drop any collections previously referenced by the alias
            for collection in old_collections:
                try:
                    ws, cname = collection.split('.')
                    rs.Collection.retrieve(
                        cname,
                        workspace=ws
                    ).drop()
                except Exception as e:
                    logger.warn(f'Failed to drop {ws}.{cname}. Continuing...')
        except Exception as e:
            if e.code == 404:
                self._create_alias(rs, alias_name, cpath)
            else:
                raise e

    def _send_rs_request(self, type, endpoint, body=None):
        url = self._rs_api_server() + endpoint
        headers = {"authorization": f'apikey {self._rs_api_key()}'}

        if type == 'GET':
            return requests.get(url, headers=headers)
        elif type == 'POST':
            return requests.post(url, headers=headers, json=body)
        elif type == 'DELETE':
            return requests.delete(url, headers=headers)
        else:
            raise Exception(f'Unimplemented request type {type}')

    def _views_endpoint(self, ws):
        return f'/v1/orgs/self/ws/{ws}/views'

    def _does_view_exist(self, ws, view):
        endpoint = self._views_endpoint(ws) + f'/{view}'
        response = self._send_rs_request('GET', endpoint)
        if response.status_code == 404:
            return False
        elif response.status_code == 200:
            return True
        else:
            raise Exception(response.text)

    def _does_alias_exist(self, ws, alias):
        rs = self._rs_client()
        try:
            rs.Alias.retrieve(
                workspace=ws,
                name=alias
            )
            return True
        except Exception as e:
            if isinstance(e, rockset.exception.InputError) and e.code == 404:
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
            if isinstance(e, rockset.exception.InputError) and e.code == 404:
                return False
            else:
                raise e

    def _create_view(self, ws, view, sql):
        # Check if alias or collection exist with same name
        rs = self._rs_client()
        if self._does_alias_exist(ws, view):
            raise Exception(f'You already have an alias {view} in workspace {ws}. Delete it and try again.')

        if self._does_collection_exist(ws, view):
            raise Exception(f'You already have a collection {view} in workspace {ws}. Delete it and try again.')

        endpoint = self._views_endpoint(ws)
        body = {
            'name': view,
            'query': sql,
            'description': 'Created via dbt'
        }
        self._send_rs_request('POST', endpoint, body=body)

    def _delete_view(self, ws, view):
        endpoint = f'{self._views_endpoint(ws)}/{view}'
        self._send_rs_request('DELETE', endpoint)
        self._wait_until_view_does_not_exist(ws, view)

    def _update_view(self, ws, view, sql):
        endpoint = self._views_endpoint(ws) + f'/{view}'
        body = {'query': sql}
        self._send_rs_request('POST', endpoint, body=body)

    def _wait_until_view_fully_synced(self, ws, view):
        endpoint = f'{self._views_endpoint(ws)}/{view}'
        while True:
            resp = self._send_rs_request('GET', endpoint)
            view_json = json.loads(resp.text)['data']
            state = view_json['state']

            if state == 'SYNCING':
                logger.info(f'Waiting for view {ws}.{view} to be fully synced')
                sleep(3)
            else:
                logger.info(f'View {ws}.{view} is synced and ready to be queried')
                break


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
    def create_table_from_external(self, schema, identifier, options):
        if not options:
            raise dbt.exceptions.CompilationException('Must provide options')

        if 'integration_name' not in options or 'bucket' not in options:
            raise dbt.exceptions.CompilationException('Must provide `integration_name` and `bucket` in options')
        iname = options['integration_name']
        rs = self._rs_client()

        # First, create the workspace if it doesn't exist
        try:
            rs.Workspace.retrieve(schema)
        except Exception as e:
            if e.code == 404 and e.type == 'NotFound':
                rs.Workspace.create(schema)
            else: # Unexpected error
                raise e

        integration = rs.Integration.retrieve(iname)
        if not integration['s3']:
            raise dbt.exceptions.CompilationException('Integration must be S3 type')
        bucket = options['bucket']

        prefix_key = 'ROCKSET_S3_STAGE_PREFIX'
        if prefix_key not in os.environ:
            raise Exception(f'Must provide {prefix_key} as an env variable')
        prefix = os.environ[prefix_key]

        # The alias that points to the collection uses the identifier name, the collection
        # uses the identifier with a timestamp
        alias_name = identifier
        cname = identifier + '_' + str(int(round(time())))

        format_params = options['format_params'] if 'format_params' in options else None
        s3 = rs.Source.s3(
            bucket=bucket,
            prefix=prefix,
            integration=integration,
            format_params=format_params
        )

        newcoll = rs.Collection.create(name=cname, workspace=schema, sources=[s3])
        self._wait_until_collection_ready(cname, schema)

        # By default, wait 5 minutes after dump
        wait_seconds_before_switch = options['wait_seconds_before_switch'] if 'wait_seconds_before_switch' in options else 300
        logger.info(f'Waiting {wait_seconds_before_switch} seconds before switching the alias to the new collection')
        sleep(wait_seconds_before_switch)

        collection_path = schema + '.' + cname
        try:
            alias = rs.Alias.retrieve(alias_name, workspace=schema)

            # If the alias is found, point it to the new collection and delete the old collection
            collections_to_drop = alias.collections
            alias.update(collections=[collection_path])

            # Wait 2 minutes before dropping the old collections, to be sure the alias has switched over
            sleep(120)

            for collection_path in collections_to_drop:
                dropping_ws, dropping_cname = collection_path.split('.')
                self._drop_collection(rs, dropping_cname, dropping_ws)
        except Exception as e:
            logger.info(e)
            # If not found, create it and point it to the collection
            if e.code == 404 and e.type == 'NotFound':
                rs.Alias.create(
                    alias_name,
                    workspace=schema,
                    collections=[collection_path])
            else: # Unexpected error
                raise e

    @available.parse(lambda *a, **k: '')
    def add_incremental_docs(self, relation, sql, unique_key):
        if unique_key and unique_key != '_id':
            raise dbt.exceptions.NotImplementedException(
                '`unique_key` can only be set to `_id` with the Rockset adapter!'
            )

        ws = relation.schema
        alias = relation.identifier

        rs = self._rs_client()
        existing_alias = rs.Alias.retrieve(alias, workspace=ws)
        cname = None
        for cpath in existing_alias.collections:
            collection_ws, collection = cpath.split('.')
            if ws == collection_ws and collection.startswith(alias):
                if cname is not None:
                    raise dbt.exceptions.Exception(f'Found multiple eligible collections referenced by {alias}; there may only be one')
                else:
                    cname = collection

        if cname is None:
            raise dbt.exceptions.Exception(
                f'Found no eligible collection referenced by {alias}; the referenced collection should be in the target ws, and be prefixed with the alias name'
            )

        # Run an INSERT INTO statement and wait for it to be fully ingested
        insert_into_sql = f'''
            INSERT INTO {ws}.{cname}
            {sql}
        '''
        iis_query_id = self._execute_query(insert_into_sql)
        self._wait_until_iis_fully_ingested(ws, cname, iis_query_id)

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

    # Execute a query not using the SQL cursor, but by hitting the REST api. This can be done
    # if you need the QueryResponse object returned
    # Returns: Query id (str)
    def _execute_query(self, sql):
        endpoint = '/v1/orgs/self/queries'
        body = {'sql':{'query': sql}}
        resp = self._send_rs_request('POST', endpoint, body=body)
        if resp.status_code != 200:
            raise dbt.exceptions.Exception(resp.text)

        return json.loads(resp.text)['query_id']

    def _wait_until_collection_does_not_exist(self, cname, ws):
        while True:
            try:
                c = self._rs_client().Collection.retrieve(cname, workspace=ws)
                logger.info(f'Waiting for collection {ws}.{cname} to be deleted...')
                sleep(5)
            except Exception as e:
                if e.code == 404 and e.type == 'NotFound': # Collection does not exist
                    return
                raise e

    def _wait_until_view_does_not_exist(self, ws, view):
        while True:
            if self._does_view_exist(ws, view):
                logger.info(f'Waiting for view {ws}.{view} to be deleted')
                sleep(3)
            else:
                break

    def _wait_until_collection_ready(self, cname, ws):
        while True:
            c = self._rs_client().Collection.retrieve(
                cname,
                workspace=ws
            )
            if c.describe().data['status'] == 'READY':
                logger.info(f'{ws}.{cname} is ready!')
                return
            else:
                logger.info(f'Waiting for collection {ws}.{cname} to become ready...')
                sleep(5)

    def _wait_until_docs(self, cname, ws, doc_count):
        while True:
            c = self._rs_client().Collection.retrieve(
                cname,
                workspace=ws
            )
            actual_count = c.describe().data['stats']['doc_count']
            if actual_count == doc_count:
                logger.info(f'{ws}.{cname} has {doc_count} docs!')
                return
            else:
                logger.info(f'Waiting for collection {ws}.{cname} to have {doc_count} docs, it has {actual_count}...')
                sleep(5)

    def _rs_alias_to_relation(self, collection):
        if collection is None:
            return None

        # define quote_policy
        quote_policy = {
            'database': False,
            'schema': True,
            'identifier': True,
        }
        return self.Relation.create(
            database=None,
            schema=collection.workspace,
            identifier=collection.name,
            type='table',
            quote_policy=quote_policy
        )

    def _drop_collection(self, rs, cname, ws):
        try:
            c = rs.Collection.retrieve(cname, workspace=ws)
            c.drop()
        except Exception as e:
            logger.info(e)
            if e.code != 404 or e.type != 'NotFound':
                raise e # Unexpected error
