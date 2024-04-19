import pytest

from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps
from dbt.tests.util import run_dbt, check_relations_equal, relation_from_name


@pytest.mark.skip()
# Rockset always used timestamps with zone information included
class TestCurrentTimestampsRockset(BaseCurrentTimestamps):
    pass


class TestSingularTestsRockset(BaseSingularTests):
    pass


class TestSingularTestsEphemeralRockset(BaseSingularTestsEphemeral):
    pass


class TestEmptyRockset(BaseEmpty):
    pass


class TestIncrementalRockset(BaseIncremental):
    pass


class TestGenericTestsRockset(BaseGenericTests):
    pass


# Snapshots unsupported
@pytest.mark.skip()
class TestSnapshotCheckColsRockset(BaseSnapshotCheckCols):
    pass


# Snapshots unsupported
@pytest.mark.skip()
class TestSnapshotTimestampRockset(BaseSnapshotTimestamp):
    pass
