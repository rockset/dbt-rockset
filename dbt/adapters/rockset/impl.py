from dbt.adapters.base import (
    BaseAdapter, available, RelationType
)
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.rockset.connections import RocksetConnectionManager
from dbt.adapters.rockset.relation import RocksetRelation
from dbt.adapters.rockset.column import RocksetColumn

import agate
import dbt
from time import sleep
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
        print('Creating workspace "{}"', relation.schema)
        rs.Workspace.create(relation.schema)

    def drop_schema(self, relation: RocksetRelation) -> None:
        rs = self._rs_client()
        print('Dropping workspace "{}"', relation.schema)
        ws = rs.Workspace.retrieve(relation.schema)
        ws.drop()

    @available.parse(lambda *a, **k: False)
    def check_schema_exists(self, database: str, schema: str) -> bool:
        rs = self._rs_client()
        try:
            _ = rs.Workspace.retrieve(schema)
            return True
        except:
            pass

        return False

    @available
    def list_schemas(self, database: str) -> List[str]:
        rs = self._rs_client()
        return [ws.name for ws in rs.Workspace.list()]

    # Relation/Collection related methods
    def truncate_relation(self, relation: RocksetRelation) -> None:
        raise dbt.exceptions.NotImplementedException(
            '`truncate` is not implemented for this adapter!'
        )

    @available.parse_list
    def drop_relation(self, relation: RocksetRelation) -> None:
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
    @available.parse(lambda *a, **k: '')
    def create_table(self, relation, sql):
        rs = self._rs_client()
        
        c = rs.Collection.create(
            relation.identifier,
            workspace=relation.schema
        )
        self._wait_until_collection_ready(relation.identifier, relation.schema)

        # Execute the given sql and parse the results
        cursor = self._rs_cursor()
        cursor.execute(sql)
        field_names = self._description_to_field_names(cursor.description)
        json_results = []
        for row in cursor.fetchall():
            json_results.append(self._row_to_json(row, field_names))

        # Write the results to the newly created table and wait until the docs are ingested
        expected_doc_count = len(json_results)
        c.add_docs(json_results)
        self._wait_until_docs(relation.identifier, relation.schema, expected_doc_count)

        return "SELECT 1"

    @available.parse(lambda *a, **k: '')
    def create_view(self, relation, sql):
        raise dbt.exceptions.NotImplementedException(
            'Rockset does not yet support views!'
        )

    ###
    # Internal Rockset helper methods
    ###

    fields_to_drop = set(['_event_time', '_id'])

    def _rs_client(self):
        return self.connections.get_thread_connection().handle._client()

    def _rs_cursor(self):
        return self.connections.get_thread_connection().handle.cursor()

    def _wait_until_collection_does_not_exist(self, cname, ws):
        while True:
            try:
                c = self._rs_client().Collection.retrieve(cname, workspace=ws)
                print(f'Waiting for collection {ws}.{cname} to be deleted...')
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
                print(f'{ws}.{cname} is ready!')
                return
            else:
                print(f'Waiting for collection {ws}.{cname} to become ready...')
                sleep(5)

    def _wait_until_docs(self, cname, ws, doc_count):
        while True:
            c = self._rs_client().Collection.retrieve(
                cname,
                workspace=ws
            )
            actual_count = c.describe().data['stats']['doc_count']
            if actual_count == doc_count:
                print(f'{ws}.{cname} has {doc_count} docs!')
                return
            else:
                print(f'Waiting for collection {ws}.{cname} to have {doc_count} docs, it has {actual_count}...')
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

    def _description_to_field_names(self, description):
        return [desc[0] for desc in description]

    def _row_to_json(self, row, field_names):
        json_res = {}
        for i in range(len(row)):
            if field_names[i] in self.fields_to_drop:
                continue
            json_res[field_names[i]] = row[i]
        return json_res
