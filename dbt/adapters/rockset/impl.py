from dbt.adapters.base import (
    BaseAdapter, available, RelationType
)
from dbt.adapters.rockset import RocksetConnectionManager
from dbt.adapters.rockset import RocksetRelation
from dbt.adapters.rockset import RocksetColumn

import agate

class RocksetAdapter(SQLAdapter):
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

    # internal Rockset helper methods
    def _rs_client(self):
        return self.connections.handle._client()

    def _rs_collection_to_relation(self, collection):
        if collection is None:
            return None

        # define quote_policy
        quote_policy = {
            'database': False,
            'schema': True,
            'identifer': True,
        }
        return self.Relation.create(
            database=None,
            schema=collection.workspace,
            identifier=collection.name,
            type='table',
            quote_policy=quote_policy
        )

    # Schema/workspace related methods
    def create_schema(self, relation: RocksetRelation) -> None:
        rs = self._rs_client()
        logger.debug('Creating workspace "{}"', relation.schema)
        rs.Workspace.create(relation.schema)

    def drop_schema(self, relation: RocksetRelation) -> None:
        rs = self._rs_client()
        logger.debug('Dropping workspace "{}"', relation.schema)
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
        [ws.name for ws in rs.Workspace.retrieve()]

    # Relation/Collection related methods
    def truncate_relation(self, relation: RocksetRelation) -> None:
        raise dbt.exceptions.NotImplementedException(
            '`truncate` is not implemented for this adapter!'
        )

    @available.parse_list
    def drop_relation(self, relation: RocksetRelation) -> None:
        rs = self._rs_client()
        c = rs.Collection.retrieve(
            relation.identifer,
            workspace=relation.schema
        )
        c.drop()

    def rename_relation(
        self, from_relation: RocksetRelation, to_relation: RocksetRelation
    ) -> None:
        raise dbt.exceptions.NotImplementedException(
            '`rename` is not implemented for this adapter!'
        )

    def get_relation(
            self, database: str, schema: str, identifier: str
        ) -> RocksetRelation:
        rs = self._rs_client()
        collection = rs.Collection.retrieve(identifer, workspace=schema)
        return self._rs_collection_to_relation(collection)

    def list_relations_without_caching(
        self, schema_relation: RocksetRelation
    ) -> List[RocksetRelation]:
            # get Rockset client to use Rockset's Python API
            rs = self._rs_client()
            if schema_relation is not None:
                collections = [rs.Collection.retrieve(
                    name=relation.identifier,
                    workspace=relation.schema,
                )]
            else:
                collections =s rs.Collection.list()

            # map Rockset collections to RocksetRelation
            relations = []
            for collection in collections:
                relations.append(self._rs_collection_to_relation(collection))

            return relations

    # Columns/fields related methods
    def get_columns_in_relation(
        self, relation: RocksetRelation
    ) -> List[RocksetColumn]:
        sql = 'DESCRIBE "{}"."{}"'.format(relation.schema, relation.identifer)
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
        pass

    def expand_target_column_types(
        self, from_relation: RocksetRelation, to_relation: RocksetRelation
    ) -> None:
        pass














