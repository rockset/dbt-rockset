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

    def get_relation(
            self, database: str, schema: str, identifier: str
        ) -> RocksetRelation:
        logger.info(f'Getting relation {schema}.{identifier}')
        try:
            rs = self._rs_client()
            collection = rs.Collection.retrieve(identifier, workspace=schema)
            return self._rs_collection_to_relation(collection)
        except Exception as e:
            if e.code == 404 and e.type == 'NotFound': # Collection does not exist
                return None
            else: # Unexpected error
                raise e

    def list_relations_without_caching(
        self, schema_relation: RocksetRelation
    ) -> List[RocksetRelation]:
        logger.info(f'Listing relations without caching')
        # get Rockset client to use Rockset's Python API
        rs = self._rs_client()
        if schema_relation and schema_relation.identifier and schema_relation.schema:
            collections = [rs.Collection.retrieve(
                schema_relation.identifier,
                workspace=schema_relation.schema,
            )]
        else:
            collections = rs.Collection.list()

        # map Rockset collections to RocksetRelation
        relations = []
        for collection in collections:
            relations.append(self._rs_collection_to_relation(collection))

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

    # Table materialization
    @available.parse(lambda *a, **k: '')
    def create_table(self, relation, sql):
        current_millis = round(time() * 1000)
        cname_new = f'{relation.identifier}_{current_millis}'
        cpath = f'{relation.schema}.{cname_new}'

        logger.info(f'Creating collection {relation.schema}.{cname_new}')
        rs = self._rs_client()

        c = rs.Collection.create(
            cname_new,
            workspace=relation.schema
        )
        self._wait_until_collection_ready(cname_new, relation.schema)

        # TODO(sam) Don't want to run the queries twice....
        # JUST GET THE ROW COUNT FROM IIS

        # Get the expected row count, so we can wait until all docs are ingested
        cursor = self._rs_cursor()
        cursor.execute(sql)
        expected_doc_count = cursor.rowcount

        # Run an INSERT INTO statement and wait for the expected doc count
        insert_into_sql = f'''
            INSERT INTO {relation.schema}.{cname_new}
            {sql}
        '''
        cursor.execute(insert_into_sql)
        self._wait_until_docs(cname_new, relation.schema, expected_doc_count)

        # Now we have a timestamp tagged table. Make sure that an alias named
        # relation.identifier is pointing to it
        alias_name = relation.identifier

        try:
            alias = rs.Alias.retrieve(alias_name, workspace=relation.schema)

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
        # Check if alias exists. If it does, delete it
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

    def _update_view(self, ws, view, sql):
        endpoint = self._views_endpoint(ws) + f'/{view}'
        body = {'query': sql}
        self._send_rs_request('POST', endpoint, body=body)

    # Table materialization
    # As of this comment, the rockset python sdk does not support views, so this is implemented
    # by hitting the api directly
    @available.parse(lambda *a, **k: '')
    def create_view(self, relation, sql):
        ws = relation.schema
        view = relation.identifier

        if not self._does_view_exist(ws, view):
            self._create_view(ws, view, sql)
        else:
            self._update_view(ws, view, sql)

        # TODO(sam) Wait for fully synced to roll out, then wait for it here
        # in addition to sleep(3)

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
        raise dbt.exceptions.NotImplementedException(
            'Rockset does not yet support incremental materializations!'
        )

        # TODO(sam): Insert a marker and wait for it to show up

        # The code below *kind of* implements incremental materializations, but I'm commenting it
        # out for now until it is better understood and tested


        # logger.info(f'Adding incremental docs to {relation.schema}.{relation.identifier}')

        # # Execute the provided sql and fetch results
        # json_results = sql_to_json_results(self._rs_cursor(), sql)

        # # TODO: Uncomment the two lines below to support using the unique_key. It works as expected,
        # # but the only thing we can't do is wait until all the new docs are ingested, bc we can't
        # # depend on final doc count when there are overrides and we have no way of knowing when the
        # # overrides complete
        # if unique_key:
        #     # self._update_ids_using_unique_key(json_results, unique_key, relation)
        #     # self._strip_internal_fields(json_results, keep_id=True)
        #     raise dbt.exceptions.NotImplementedException(
        #         '`unique_key` not yet supported for the Rockset adapter!'
        #     )
        # else:
        #     c = self._rs_client().Collection.retrieve(
        #         relation.identifier,
        #         workspace=relation.schema
        #     )
        #     initial_doc_count = c.describe()['data']['stats']['doc_count']

        #     self._strip_internal_fields(json_results)
        #     expected_doc_count = initial_doc_count + len(json_results)

        #  # Write the docs to the created table and wait until they are ingested
        # c.add_docs(json_results)
        # self._wait_until_docs(relation.identifier, relation.schema, expected_doc_count)

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

    def _rs_collection_to_relation(self, collection):
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

    def _strip_internal_fields(self, json_results, keep_id=False):
        fields_to_drop = ['_event_time', '_meta']

        if not keep_id:
            fields_to_drop.append('_id')

        for res in json_results:
            for field in fields_to_drop:
                res.pop(field, None)

    def _update_ids_using_unique_key(self, json_results, unique_key, target_relation):
        sql = f'SELECT * FROM {target_relation.schema}.{target_relation.identifier}'
        target_collection_docs = sql_to_json_results(self._rs_cursor(), sql)

        unique_key_val_to_id = {}
        for doc in target_collection_docs:
            if unique_key in doc:
                unique_key_val = str(doc[unique_key])
                unique_key_val_to_id[unique_key_val] = doc['_id']

        for res in json_results:
            if unique_key in res and str(res[unique_key]) in unique_key_val_to_id:
                res['_id'] = unique_key_val_to_id[str(res[unique_key])]

    def _drop_collection(self, rs, cname, ws):
        try:
            c = rs.Collection.retrieve(cname, workspace=ws)
            c.drop()
        except Exception as e:
            logger.info(e)
            if e.code != 404 or e.type != 'NotFound':
                raise e # Unexpected error
