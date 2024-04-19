import pytest
import os
import random, time
from rockset import *
from rockset.models import *
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod, models__expected_sql, models__upstream_sql, models__model_sql
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.util import run_dbt, check_relations_equal, relation_from_name, check_result_nodes_by_name, get_manifest, check_relation_types
from dbt.tests.fixtures.project import write_project_files
from dbt.tests.util import run_dbt, check_relations_equal

models__base_sql = """
select 1 as num
UNION
select 2 as num
"""


# Create some tags
tags = [f"rand_tag_{i}_" + str(random.randint(0, 10**4)) for i in range(3)]

models__ql_sql = """
{{ config(
        materialized="query_lambda",
""" + f"tags={tags}," + """
        parameters=[
            {'name': 'mul', 'value': '7', 'type': 'int' },
            {'name': 'exclude', 'value': '1', 'type': 'int' },
            ],
        )}}
select num * :mul as result
from {{ ref('base') }}
where num <= :exclude
"""

ql_name = 'dbt_ql'

class TestQueryLambdaCreationRockset(BaseAdapterMethod):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base.sql": models__base_sql,
            f"{ql_name}.sql": models__ql_sql,
        }
   
    def test_adapter_methods(self, project, equal_tables):
        run_dbt(["compile"])  # trigger any compile-time issues
        result = run_dbt()
        workspace = result.results[0].node.schema
        rs = RocksetClient(api_key=os.getenv('API_KEY'), host=os.getenv('API_SERVER'))
        resp = rs.QueryLambdas.get_query_lambda_tag_version(query_lambda=ql_name, workspace=workspace, tag=tags[0])
        ql_version = resp.data.version.version
        tag_resp = rs.QueryLambdas.list_query_lambda_tags(query_lambda=ql_name, workspace=workspace)
        returned_tags = {x.tag_name for x in  tag_resp.data}
        assert returned_tags == (set(tags) | {'latest'}), "Expected tags must match"

        exec_resp = rs.QueryLambdas.execute_query_lambda(query_lambda=ql_name, version=ql_version, workspace=workspace)
        assert exec_resp.results == [{'result': 7}], "QL result must match"

class TestQueryLambdaUpdatesRockset(BaseAdapterMethod):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base.sql": models__base_sql,
            f"{ql_name}.sql": models__ql_sql,
        }
   
    def test_adapter_methods(self, project, equal_tables):
        result = run_dbt(["compile"])  # trigger any compile-time issues
        workspace = result.results[0].node.schema
        # Create a ql, so the dbt created one must be updated
        rs = RocksetClient(api_key=os.getenv('API_KEY'), host=os.getenv('API_SERVER'))
        # Wait for workspace to be created from dbt compile
        for _ in range(10):
            time.sleep(1)
            if workspace in {ws.name for ws in rs.Workspaces.list().data}:
                break
        resp = rs.QueryLambdas.create_query_lambda(name=ql_name, workspace=workspace, 
                                            sql=QueryLambdaSql(query='SELECT 1', default_parameters=[]))
        result = run_dbt()
        resp = rs.QueryLambdas.get_query_lambda_tag_version(query_lambda=ql_name, workspace=workspace, tag=tags[1])
        ql_version = resp.data.version.version
        tag_resp = rs.QueryLambdas.list_query_lambda_tags(query_lambda=ql_name, workspace=workspace)
        returned_tags = {x.tag_name for x in  tag_resp.data}
        assert returned_tags == (set(tags) | {'latest'}), "Expected tags must match"

        exec_resp = rs.QueryLambdas.execute_query_lambda(query_lambda=ql_name, version=ql_version, workspace=workspace)
        assert exec_resp.results == [{'result': 7}], "QL result must match"
        ql_versions = rs.QueryLambdas.list_query_lambda_versions(query_lambda=ql_name, workspace=workspace)
        assert len(ql_versions.data) == 2, "Two version have been created"

