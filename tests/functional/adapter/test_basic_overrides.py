import pytest
import os
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod, models__expected_sql, models__upstream_sql, models__model_sql
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.util import run_dbt, check_relations_equal, relation_from_name, check_result_nodes_by_name, get_manifest, check_relation_types


class TestBaseAdapterMethodRockset(BaseAdapterMethod):
    @pytest.fixture(scope="class")
    def tests(self):
        # RS cannot get columns of a relation during compilation
        return {}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": models__upstream_sql,
            "expected.sql": models__expected_sql,
            # RS cannot get columns of a view, must materialize the model
            "model.sql": """
                       {{ config(materialized="table") }}
                        """ + models__model_sql,
        }
    pass


class TestEphemeralRockset(BaseEphemeral):
   # RS cannot generate catalog entries from views, requires an override
    def test_ephemeral(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["base"])

        # run command
        results = run_dbt(["run"])
        assert len(results) == 2
        check_result_nodes_by_name(results, ["view_model", "table_model"])

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations equal
        check_relations_equal(
            project.adapter, ["base", "view_model", "table_model"])

        # catalog node count
        catalog = run_dbt(["docs", "generate"])
        catalog_path = os.path.join(
            project.project_root, "target", "catalog.json")
        assert os.path.exists(catalog_path)
        # views are not in the catalog
        assert len(catalog.nodes) == 2
        assert len(catalog.sources) == 1

        # manifest (not in original)
        manifest = get_manifest(project.project_root)
        assert len(manifest.nodes) == 4
        assert len(manifest.sources) == 1

    pass


class TestSimpleMaterializationsRockset(BaseSimpleMaterializations):
   # RS cannot generate catalog entries from views, requires an override
    def test_base(self, project):

        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 3

        # names exist in result nodes
        check_result_nodes_by_name(
            results, ["view_model", "table_model", "swappable"])

        # check relation types
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        check_relations_equal(
            project.adapter, ["base", "view_model", "table_model", "swappable"])

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        # views aren't in the catalog
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to view
        # required for BigQuery
        if project.test_config.get("require_full_refresh", False):
            results = run_dbt(
                ["run", "--full-refresh", "-m", "swappable",
                    "--vars", "materialized_var: view"]
            )
        else:
            results = run_dbt(
                ["run", "-m", "swappable", "--vars", "materialized_var: view"])
        assert len(results) == 1

        # check relation types, swappable is view
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "view",
        }
        check_relation_types(project.adapter, expected)

        # run_dbt changing materialized_var to incremental
        results = run_dbt(["run", "-m", "swappable", "--vars",
                          "materialized_var: incremental"])
        assert len(results) == 1

        # check relation types, swappable is table
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

    pass
