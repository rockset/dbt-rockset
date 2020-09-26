from dbt.adapters.rockset.connections import RocksetConnectionManager
from dbt.adapters.rockset.connections import RocksetCredentials
from dbt.adapters.rockset.impl import RocksetAdapter
from dbt.adapters.rockset.relation import RocksetRelation
from dbt.adapters.rockset.column import RocksetColumn

from dbt.adapters.base import AdapterPlugin
from dbt.include import rockset


Plugin = AdapterPlugin(
    adapter=RocksetAdapter,
    credentials=RocksetCredentials,
    include_path=rockset.PACKAGE_PATH)
